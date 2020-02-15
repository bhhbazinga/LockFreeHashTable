#ifndef LOCKFREE_HASHTABLE_H
#define LOCKFREE_HASHTABLE_H

#include <atomic>
#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>

#define LOG(fmt, ...)                  \
  do {                                 \
    fprintf(stderr, fmt, __VA_ARGS__); \
  } while (0)

#include "reclaimer.h"

// Total bucket size equals to kSegmentSize^kMaxLevel, in this case total bucket
// size is 2^40, if the load factor is 0.5, Hash Table can be stored 2^40 * 0.5
// = 2^39 items. You can adjust the following two values according to your
// memory size.
const int kMaxLevel = 1 << 2;
const int kSegmentSize = 1 << 10;

// Hash Table can be stored 2^power_of_2_ * kLoadFactor items.
const float kLoadFactor = 0.5;

template <typename K, typename V, typename Hash = std::hash<K>>
class LockFreeHashTable {
  struct Node;
  struct Segment;

  typedef size_t HashKey;
  typedef size_t BucketIndex;
  typedef size_t SegmentIndex;
  typedef std::atomic<Node*> Bucket;

 public:
  void Dump() {
    Node* p = GetBucketHeadByIndex(0);
    while (p) {
      if (p->IsDummy()) {
        LOG("hash=%lu,bucket=%lu---->", p->hash, p->bucket);
      }
      // LOG("dummy=%d,hash=%lu,bucket=%lu---->", p->IsDummy(),
      //     p->hash, p->bucket);
      p = p->next;
    }
    LOG("%s", "\n");
  }

  LockFreeHashTable() : power_of_2_(1), size_(0), hash_func_(Hash()) {
    // Initialize first bucket
    int level = 1;
    Segment* segments = segments_;  // Point to current segment.
    while (level++ <= kMaxLevel - 2) {
      Segment* child_segments = NewSegments(level);
      segments[0].data.store(child_segments, std::memory_order_relaxed);
      segments = child_segments;
    }

    Bucket* buckets = NewBuckets();
    segments[0].data.store(buckets, std::memory_order_relaxed);

    Node* head = new Node(0);
    buckets[0].store(head, std::memory_order_relaxed);
    head->bucket = 0;
  }

  void Insert(const K& key, const V& value) {
    Node* new_node = new Node(key, value, hash_func_);
    Node* head = GetBucketHeadByHash(new_node->hash);
    InsertNode(head, new_node);
  }

  void Insert(K&& key, const V& value) {
    Node* new_node = new Node(std::move(key), value, hash_func_);
    Node* head = GetBucketHeadByHash(new_node->hash);
    InsertNode(head, new_node);
  }

  void Insert(const K& key, V&& value) {
    Node* new_node = new Node(key, std::move(value), hash_func_);
    Node* head = GetBucketHeadByHash(new_node->hash);
    InsertNode(head, new_node);
  }

  void Insert(K&& key, V&& value) {
    Node* new_node = new Node(std::move(key), std::move(value), hash_func_);
    Node* head = GetBucketHeadByHash(new_node->hash);
    new_node->bucket = (new_node->hash) % bucket_size();
    InsertNode(head, new_node);
  }

  bool Delete(const K& key) {
    HashKey hash = hash_func_(key);
    Node* head = GetBucketHeadByHash(hash);
    Node delete_node(key, hash_func_);
    return DeleteNode(head, &delete_node);
  }

  bool Find(const K& key, V& value) {
    HashKey hash = hash_func_(key);
    Node* head = GetBucketHeadByHash(hash);
    Node find_node(key, hash_func_);
    return FindNode(head, &find_node, value);
  };

  size_t size() const { return size_.load(std::memory_order_consume); }
  size_t bucket_size() const {
    return 1 << power_of_2_.load(std::memory_order_consume);
  }

 private:
  Segment* NewSegments(int level) {
    Segment* segments = new Segment[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      segments[i].level = level;
      segments[i].data.store(nullptr, std::memory_order_relaxed);
    }
    return segments;
  }

  Bucket* NewBuckets() {
    Bucket* buckets = new Bucket[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      buckets[i].store(nullptr, std::memory_order_relaxed);
    }
    return buckets;
  }

  Node* InitializeBucket(BucketIndex bucket_index);

  // When the table size is 2^i , a logical table bucket b contains items whose
  // keys k maintain k mod 2^i = b. When the size becomes 2^i+1, the items of
  // this bucket are split into two buckets: some remain in the bucket b, and
  // others, for which k mod 2^(i+1) == b + 2^i.
  // So, the index of parent of bucket b equals to b - 2^i.
  BucketIndex GetBucketParent(BucketIndex bucket_index) const {
    //__builtin_clzl: Get number of leading zero bits.
    // Unset the MSB(most significant bit) of bucket_index;
    return (~(0x8000000000000000 >> (__builtin_clzl(bucket_index))) & bucket_index);
  };

  // Get the head node of bucket, if bucket not exist then return nullptr or
  // return head.
  Node* GetBucketHeadByIndex(BucketIndex bucket_index);

  // Get the head node of bucket, if bucket not exist then initialize it and
  // return head.
  Node* GetBucketHeadByHash(HashKey hash) {
    BucketIndex bucket_index = hash % bucket_size();
    Node* head = GetBucketHeadByIndex(bucket_index);
    if (nullptr == head) {
      head = InitializeBucket(bucket_index);
    }
    return head;
  }

  // Harris' OrderedListBasedset with hazard pointer to manage memory.
  void InsertNode(Node* head, Node* new_node);
  bool DeleteNode(Node* head, Node* delete_node);
  bool FindNode(Node* head, Node* find_node, V& value) {
    Node* prev;
    Node* cur;
    bool found = SearchNode(head, find_node, &prev, &cur);
    if (found) {
      value = *cur->value.load(std::memory_order_consume);
    }
    ClearHazardPointer();
    return found;
  }

  static HashKey Reverse(HashKey hash) {
    return lookuptable_[hash & 0xff] << 56 |
           lookuptable_[(hash >> 8) & 0xff] << 48 |
           lookuptable_[(hash >> 16) & 0xff] << 40 |
           lookuptable_[(hash >> 24) & 0xff] << 32 |
           lookuptable_[(hash >> 32) & 0xff] << 24 |
           lookuptable_[(hash >> 40) & 0xff] << 16 |
           lookuptable_[(hash >> 48) & 0xff] << 8 |
           lookuptable_[(hash >> 56) & 0xff];
  }
  static HashKey RegularKey(HashKey hash) { return Reverse(hash | 1ul << 63); }
  static HashKey DummyKey(HashKey hash) { return Reverse(hash); }

  bool SearchNode(const Node* head, const Node* search_node, Node** prev_ptr,
                  Node** cur_ptr);

  bool Less(const Node* node1, const Node* node2) const {
    if (node1->reverse_hash != node2->reverse_hash) {
      return node1->reverse_hash < node2->reverse_hash;
    }
    return *node1->key < *node2->key;
  }

  bool GreaterOrEquals(const Node* node1, const Node* node2) const {
    return !(Less(node1, node2));
  }

  bool Equals(const Node* node1, const Node* node2) const {
    return !Less(node1, node2) && !Less(node2, node1);
  }

  bool is_marked_reference(Node* next) const {
    return (reinterpret_cast<unsigned long>(next) & 0x1) == 0x1;
  }

  Node* get_marked_reference(Node* next) const {
    return reinterpret_cast<Node*>(reinterpret_cast<unsigned long>(next) | 0x1);
  }

  Node* get_unmarked_reference(Node* next) const {
    return reinterpret_cast<Node*>(reinterpret_cast<unsigned long>(next) &
                                   ~0x1);
  }

  static void OnDeleteNode(void* ptr) { delete static_cast<Node*>(ptr); }

  // After invoke Search, we should clear hazard pointer,
  // invoke ClearHazardPointer after Insert and Delete.
  void ClearHazardPointer() {
    Reclaimer& reclaimer = Reclaimer::GetInstance();
    reclaimer.MarkHazard(0, nullptr);
    reclaimer.MarkHazard(1, nullptr);
  }

  struct Node {
    Node()
        : hash(0),
          reverse_hash(0),
          key(nullptr),
          value(nullptr),
          next(nullptr) {}

    // used to initialize dummy node
    Node(HashKey hash_)
        : hash(hash_),
          reverse_hash(DummyKey(hash)),
          key(nullptr),
          value(nullptr),
          next(nullptr) {}

    // used to initialize regular node
    Node(const K& key_, V&& value_, const Hash& hash_func)
        : hash(hash_func(key_)),
          reverse_hash(RegularKey(hash)),
          key(new K(key_)),
          value(new V(std::move(value_))),
          next(nullptr) {}
    Node(const K& key_, const V& value_, const Hash& hash_func)
        : hash(hash_func(key_)),
          reverse_hash(RegularKey(hash)),
          key(new K(key_)),
          value(new V(value_)),
          next(nullptr) {}
    Node(K&& key_, const V& value_, const Hash& hash_func)
        : hash(hash_func(key_)),
          reverse_hash(RegularKey(hash)),
          key(new K(std::move(key_))),
          value(new V(value_)),
          next(nullptr) {}
    Node(K&& key_, V&& value_, const Hash& hash_func)
        : hash(hash_func(key_)),
          reverse_hash(RegularKey(hash)),
          key(new K(std::move(key_))),
          value(new V(std::move(value_))),
          next(nullptr) {}

    // used to compare in search
    Node(const K& key_, const Hash& hash_func)
        : hash(hash_func(key_)),
          reverse_hash(RegularKey(hash)),
          key(new K(key_)),
          value(nullptr),
          next(nullptr) {}

    ~Node() {
      V* ptr = value.load(std::memory_order_consume);
      if (ptr != nullptr) delete ptr;
      if (key != nullptr) delete key;
    }

    void Dump() {
      LOG("is_dummy=%d,hash=%lu,reverse_hash=%lu\n", IsDummy(), hash,
          reverse_hash);
    }

    bool IsDummy() { return (reverse_hash & 0x1) == 0; }

    HashKey hash;
    HashKey reverse_hash;
    BucketIndex bucket;
    K* key;
    std::atomic<V*> value;
    std::atomic<Node*> next;
  };

  struct Segment {
    Segment() : level(1), data(nullptr) {}
    Segment(int level_) : level(level_), data(nullptr) {}

    ~Segment() {
      // void* ptr = data.load(std::memory_order_consume);
      // if (nullptr == ptr) return;

      // if (level == kMaxLevel) {
      //   delete[] static_cast<Bucket*>(ptr);
      // } else {
      //   delete static_cast<Segment*>(ptr);
      // }
    }

    int level;                // Level of segment.
    std::atomic<void*> data;  // If level == kMaxLevel then data point to
                              // buckets else data point to segments.
  };

  std::atomic<size_t> power_of_2_;  // bucket size == 2^power_of_2_
  std::atomic<size_t> size_;        // item size
  Hash hash_func_;                  // hash function
  Segment segments_[kSegmentSize];  // top level sengments
  static size_t lookuptable_[256];  // lookup table for reverse bits quickly
};

// Fast reverse bits using Lookup Table.
#define R2(n) n, n + 2 * 64, n + 1 * 64, n + 3 * 64
#define R4(n) R2(n), R2(n + 2 * 16), R2(n + 1 * 16), R2(n + 3 * 16)
#define R6(n) R4(n), R4(n + 2 * 4), R4(n + 1 * 4), R4(n + 3 * 4)
// Lookup Table that store the reverse of each 8bit number.
template <typename K, typename V, typename Hash>
size_t LockFreeHashTable<K, V, Hash>::lookuptable_[256] = {R6(0), R6(2), R6(1),
                                                           R6(3)};

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::Node*
LockFreeHashTable<K, V, Hash>::InitializeBucket(BucketIndex bucket_index) {
  BucketIndex parent_index = GetBucketParent(bucket_index);
  LOG("parent_index=%lu\n", parent_index);
  if (bucket_index == 2) assert(parent_index == 0);
  Node* parent_head = GetBucketHeadByIndex(parent_index);
  if (nullptr == parent_head) {
    parent_head = InitializeBucket(parent_index);
  }

  int level = 1;
  SegmentIndex segment_index = bucket_index;
  Segment* segments = segments_;  // Point to current segment.
  while (level++ <= kMaxLevel - 2) {
    segment_index /= kSegmentSize;
    std::atomic<void*>& data = segments[segment_index].data;
    Segment* child_segments =
        static_cast<Segment*>(data.load(std::memory_order_consume));
    if (nullptr == child_segments) {
      // Try allocate segments.
      child_segments = NewSegments(level);
      void* expected = nullptr;
      if (!data.compare_exchange_strong(expected, child_segments,
                                        std::memory_order_release)) {
        delete[] child_segments;
        child_segments = static_cast<Segment*>(expected);
      }
    }
    segments = child_segments;
  }

  segment_index /= kSegmentSize;
  std::atomic<void*>& data = segments[segment_index].data;
  Bucket* buckets = static_cast<Bucket*>(data.load(std::memory_order_consume));
  if (nullptr == buckets) {
    // Try allocate buckets.
    void* expected = nullptr;
    buckets = NewBuckets();
    if (!data.compare_exchange_strong(expected, buckets,
                                      std::memory_order_release)) {
      delete[] buckets;
      buckets = static_cast<Bucket*>(expected);
    }
  }

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  Node* head = bucket.load(std::memory_order_consume);
  if (nullptr == head) {
    // Try allocate dummy head.
    head = new Node(bucket_index);
    Node* expected = nullptr;
    if (!buckets[bucket_index].compare_exchange_strong(
            expected, head, std::memory_order_release)) {
      delete head;
      head = expected;
    }
    head->bucket = bucket_index;
    InsertNode(parent_head, head);
  }
  return head;
}

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::Node*
LockFreeHashTable<K, V, Hash>::GetBucketHeadByIndex(BucketIndex bucket_index) {
  int level = 1;
  SegmentIndex segment_index = bucket_index;
  const Segment* segments = segments_;
  while (level++ <= kMaxLevel - 2) {
    segment_index /= kSegmentSize;
    segments = static_cast<Segment*>(
        segments[segment_index].data.load(std::memory_order_consume));
    if (nullptr == segments) return nullptr;
  }

  segment_index /= kSegmentSize;
  Bucket* buckets = static_cast<Bucket*>(
      segments[segment_index].data.load(std::memory_order_consume));
  if (nullptr == buckets) return nullptr;

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  // LOG("index=%d,bucket=%p\n", bucket_index % kSegmentSize,
  // bucket.load(std::memory_order_consume); LOG("index=%d,bucket=%p\n",
  // bucket_index % kSegmentSize); if (bucket_index == 2) {
  //   LOG("bucket=%p\n", bucket.load(std::memory_order_consume));
  // }

  return bucket.load(std::memory_order_consume);
}

template <typename K, typename V, typename Hash>
void LockFreeHashTable<K, V, Hash>::InsertNode(Node* head, Node* new_node) {
  Node* prev;
  Node* cur;
  do {
    if (SearchNode(head, new_node, &prev, &cur)) {
      // If list already contains *new_node->value,
      // then update value to new value.
      ClearHazardPointer();
      if (new_node == cur)
        return;  // If currently initialize same bucket, the return may be
                 // triggered.

      // At this point, new_node is created by the current thread.
      // So we can safely use memory_order_relaxed.
      V* new_value = new_node->value.load(std::memory_order_relaxed);
      V* old_value = cur->value.exchange(new_value, std::memory_order_release);
      delete old_value;
      new_node->value.store(nullptr, std::memory_order_relaxed);
      delete new_node;
      return;
    }

    new_node->next.store(cur, std::memory_order_release);
  } while (!prev->next.compare_exchange_weak(
      cur, new_node, std::memory_order_release, std::memory_order_relaxed));
  ClearHazardPointer();

  if (!new_node->IsDummy()) {
    size_t size = size_.fetch_add(1, std::memory_order_release) + 1;
    size_t power = power_of_2_.load(std::memory_order_consume);
    if ((1 << power) * kLoadFactor < size) {
      power_of_2_.compare_exchange_strong(power, power + 1,
                                          std::memory_order_release);
    }
  }
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::SearchNode(const Node* head,
                                               const Node* search_node,
                                               Node** prev_ptr,
                                               Node** cur_ptr) {
  Reclaimer& reclaimer = Reclaimer::GetInstance();
try_again:
  Node* prev = const_cast<Node*>(head);
  Node* cur = prev->next.load(std::memory_order_acquire);
  Node* next;
  while (true) {
    reclaimer.MarkHazard(0, cur);
    // Make sure prev is the predecessor of cur,
    // so that cur is properly marked as hazard.
    if (prev->next.load(std::memory_order_acquire) != cur) goto try_again;

    if (nullptr == cur || cur->IsDummy()) {
      *prev_ptr = prev;
      *cur_ptr = cur;
      return false;
    }

    next = cur->next.load(std::memory_order_acquire);
    if (is_marked_reference(next)) {
      if (!prev->next.compare_exchange_strong(cur,
                                              get_unmarked_reference(next)))
        goto try_again;

      reclaimer.ReclaimLater(cur, LockFreeHashTable<K, V, Hash>::OnDeleteNode);
      reclaimer.ReclaimNoHazardPointer();
      size_.fetch_sub(1, std::memory_order_release);
      cur = get_unmarked_reference(next);
    } else {
      Node copy_cur(*cur->key, hash_func_);  // Copy *cur for compare.
      // Make sure prev is the predecessor of cur,
      // so that copy_cur is correct.
      if (prev->next.load(std::memory_order_acquire) != cur) goto try_again;

      // Can not get copy_cur after above invocation,
      // because prev may not be the predecessor of cur at this point.
      if (GreaterOrEquals(&copy_cur, search_node)) {
        *prev_ptr = prev;
        *cur_ptr = cur;
        return Equals(&copy_cur, search_node);
      }
      // swap two hazard pointers,
      // at this point, hp0 point to cur, hp1 point to prev
      void* hp0 = reclaimer.GetHazardPtr(0);
      void* hp1 = reclaimer.GetHazardPtr(1);
      reclaimer.MarkHazard(2, hp0);  // Temporarily save hp0.
      reclaimer.MarkHazard(0, hp1);
      reclaimer.MarkHazard(1, hp0);
      reclaimer.MarkHazard(2, nullptr);
      // at this point, hp0 point to prev, hp1 point to cur

      prev = cur;
      cur = next;
    }
  };

  assert(false);
  return false;
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::DeleteNode(Node* head, Node* delete_node) {
  Node* prev;
  Node* cur;
  Node* next;
  do {
    do {
      if (!SearchNode(head, delete_node, &prev, &cur)) {
        ClearHazardPointer();
        return false;
      }
      next = cur->next.load(std::memory_order_acquire);
    } while (is_marked_reference(next));
    // Logically delete cur by marking cur->next.
  } while (!cur->next.compare_exchange_weak(next, get_marked_reference(next),
                                            std::memory_order_release,
                                            std::memory_order_relaxed));

  if (prev->next.compare_exchange_strong(cur, next, std::memory_order_release,
                                         std::memory_order_relaxed)) {
    size_.fetch_sub(1, std::memory_order_release);
    Reclaimer& reclaimer = Reclaimer::GetInstance();
    reclaimer.ReclaimLater(cur, LockFreeHashTable<K, V, Hash>::OnDeleteNode);
    reclaimer.ReclaimNoHazardPointer();
  } else {
    SearchNode(head, delete_node, &prev, &cur);
  }

  ClearHazardPointer();
  return true;
}

#endif  // LOCKFREE_HASHTABLE_H