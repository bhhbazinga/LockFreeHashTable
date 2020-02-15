#ifndef RECLAIMER_H
#define RECLAIMER_H

#include <atomic>
#include <cassert>
#include <functional>
#include <thread>
#include <unordered_map>
#include <unordered_set>

// A coefficient that used to calcuate the max number
// of reclaim node in reclaim list.
const int kCoefficient = 4 + 1 / 4;

// The number of hazard pointers of each thread
// is kHarzardPointersPerThread, you should change
// this value according to your requirement.
const int kHarzardPointersPerThread = 3;

struct HazardPointer {
  HazardPointer() : ptr(nullptr), next(nullptr) {}
  ~HazardPointer() {}

  HazardPointer(const HazardPointer& other) = delete;
  HazardPointer(HazardPointer&& other) = delete;
  HazardPointer& operator=(const HazardPointer& other) = delete;
  HazardPointer& operator=(HazardPointer&& other) = delete;

  std::atomic_flag flag;
  // We must use atomic pointer to ensure the modification
  // is visible to other threads.
  std::atomic<void*> ptr;
  HazardPointer* next;
};

struct HazardPointerList {
  HazardPointerList() : head(new HazardPointer()) {}
  ~HazardPointerList() {
    // HazardPointerList destruct when program exit.
    HazardPointer* p = head.load(std::memory_order_consume);
    while (p) {
      HazardPointer* temp = p;
      p = p->next;
      delete temp;
    }
  }

  size_t get_size() const { return size.load(std::memory_order_relaxed); }

  std::atomic<HazardPointer*> head;
  std::atomic<size_t> size;
};

// Global HazardPointerList, each thread checks this structure
// to determine if a pointer is hazard.
static HazardPointerList g_hazard_pointer_list;

class Reclaimer {
 public:
  static Reclaimer& GetInstance() {
    // Each thread has its own reclaimer.
    thread_local static Reclaimer reclaimer;
    return reclaimer;
  }

  Reclaimer(const Reclaimer&) = delete;
  Reclaimer(Reclaimer&&) = delete;
  Reclaimer& operator=(const Reclaimer&) = delete;
  Reclaimer& operator=(Reclaimer&&) = delete;

  // Use an hazard pointer at the index of hazard_pointers_ array to mark a ptr
  // as an hazard pointer pointer.
  void MarkHazard(int index, void* const ptr) {
    assert(index < kHarzardPointersPerThread);
    hazard_pointers_[index]->ptr.store(ptr, std::memory_order_release);
  }

  // Get ptr that marked as hazard at the index of hazard_pointers_ array.
  void* GetHazardPtr(int index) {
    assert(index < kHarzardPointersPerThread);
    return hazard_pointers_[index]->ptr.load(std::memory_order_consume);
  }

  // Check if the ptr is hazard.
  bool Hazard(void* const ptr) {
    std::atomic<HazardPointer*>& head = g_hazard_pointer_list.head;
    HazardPointer* p = head.load(std::memory_order_consume);
    do {
      if (p->ptr.load(std::memory_order_consume) == ptr) {
        return true;
      }
      p = p->next;
    } while (p);

    return false;
  }

  // If ptr is hazard then reclaim it later.
  void ReclaimLater(void* const ptr, std::function<void(void*)>&& func) {
    ReclaimNode* new_node = reclaim_pool_.Pop();
    new_node->ptr = ptr;
    new_node->delete_func = std::move(func);
    reclaim_map_.insert(std::make_pair(ptr, new_node));
  }

  // Try to reclaim all no hazard pointers.
  void ReclaimNoHazardPointer() {
    if (reclaim_map_.size() < kCoefficient * g_hazard_pointer_list.get_size()) {
      return;
    }

    // Used to speed up the inspection of the ptr.
    std::unordered_set<void*> not_allow_delete_set;
    std::atomic<HazardPointer*>& head = g_hazard_pointer_list.head;
    HazardPointer* p = head.load(std::memory_order_consume);
    do {
      void* const ptr = p->ptr.load(std::memory_order_consume);
      if (nullptr != ptr) {
        not_allow_delete_set.insert(ptr);
      }
      p = p->next;
    } while (p);

    for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
      if (not_allow_delete_set.find(it->first) == not_allow_delete_set.end()) {
        ReclaimNode* node = it->second;
        node->delete_func(node->ptr);
        reclaim_pool_.Push(node);
        it = reclaim_map_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  Reclaimer() {
    std::atomic<HazardPointer*>& head = g_hazard_pointer_list.head;

    for (int i = 0; i < kHarzardPointersPerThread; ++i) {
      HazardPointer* p = head.load(std::memory_order_consume);
      HazardPointer* hp = nullptr;
      do {
        // Try to get the idle hazard pointer.
        if (!p->flag.test_and_set()) {
          hp = p;
          break;
        }
        p = p->next;
      } while (p);

      if (nullptr == hp) {
        // No idle hazard pointer, allocate new one.
        HazardPointer* new_head = new HazardPointer();
        new_head->flag.test_and_set();
        hp = new_head;
        g_hazard_pointer_list.size.fetch_add(1, std::memory_order_relaxed);
        HazardPointer* old_head = head.load(std::memory_order_relaxed);
        do {
          new_head->next = old_head;
        } while (!head.compare_exchange_weak(old_head, new_head,
                                             std::memory_order_release,
                                             std::memory_order_relaxed));
      }

      hazard_pointers_[i] = hp;
    }

    for (int i = 0; i < kHarzardPointersPerThread; ++i) {
      assert(hazard_pointers_[i] != nullptr);
    }
  }

  ~Reclaimer() {
    // The Reclaimer destruct when the thread exit

    // 1.Hand over the hazard pointer
    for (int i = 0; i < kHarzardPointersPerThread; ++i) {
      assert(nullptr ==
             hazard_pointers_[i]->ptr.load(std::memory_order_relaxed));
      hazard_pointers_[i]->flag.clear();
    }

    // 2.Make sure reclaim all no hazard pointers
    for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
      // Wait until pointer is no hazard
      while (Hazard(it->first)) {
        std::this_thread::yield();
      }

      ReclaimNode* node = it->second;
      node->delete_func(node->ptr);
      delete node;
      it = reclaim_map_.erase(it);
    }
  }

  struct ReclaimNode {
    ReclaimNode() : ptr(nullptr), next(nullptr), delete_func(nullptr) {}
    ~ReclaimNode() {}

    void* ptr;
    ReclaimNode* next;
    std::function<void(void*)> delete_func;
  };

  // Reuse ReclaimNode
  struct ReclaimPool {
    ReclaimPool() : head(new ReclaimNode()) {}
    ~ReclaimPool() {
      ReclaimNode* temp;
      while (head) {
        temp = head;
        head = head->next;
        delete temp;
      }
    }

    void Push(ReclaimNode* node) {
      node->next = head;
      head = node;
    }

    ReclaimNode* Pop() {
      if (nullptr == head->next) {
        return new ReclaimNode();
      }

      ReclaimNode* temp = head;
      head = head->next;
      temp->next = nullptr;
      return temp;
    }

    ReclaimNode* head;
  };

  HazardPointer* hazard_pointers_[kHarzardPointersPerThread];

  std::unordered_map<void*, ReclaimNode*> reclaim_map_;
  ReclaimPool reclaim_pool_;
};
#endif  // RECLAIMER_H
