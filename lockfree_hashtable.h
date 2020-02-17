#ifndef LOCKFREE_HASHTABLE_H
#define LOCKFREE_HASHTABLE_H

#include <atomic>
#include <cassert>
#include <cstdio>

#define LOG(fmt, ...)                  \
  do {                                 \
    fprintf(stderr, fmt, __VA_ARGS__); \
  } while (0)

#include "reclaimer.h"

// Total bucket size equals to kSegmentSize^kMaxLevel, in this case the total
// bucket size is 64^4. If the load factor is 0.5, the maximum number of
// items that Hash Table contains is 64^4 * 0.5 = 2^23. You can adjust the
// following two values according to your memory size.
const int kMaxLevel = 4;
const int kSegmentSize = 64;

// Hash Table can be stored 2^power_of_2_ * kLoadFactor items.
const float kLoadFactor = 0.5;

template <typename K, typename V, typename Hash = std::hash<K>>
class LockFreeHashTable {
  struct Node;
  struct DummyNode;
  struct RegularNode;
  struct Segment;

  typedef size_t HashKey;
  typedef size_t BucketIndex;
  typedef size_t SegmentIndex;
  typedef std::atomic<DummyNode*> Bucket;

 public:
  LockFreeHashTable() : power_of_2_(1), size_(0), hash_func_(Hash()) {
    // Initialize first bucket
    int level = 1;
    Segment* segments = segments_;  // Point to current segment.
    while (level++ <= kMaxLevel - 2) {
      Segment* sub_segments = NewSegments(level);
      segments[0].data.store(sub_segments, std::memory_order_release);
      segments = sub_segments;
    }

    Bucket* buckets = NewBuckets();
    segments[0].data.store(buckets, std::memory_order_release);

    DummyNode* head = new DummyNode(0);
    buckets[0].store(head, std::memory_order_release);
  }

  ~LockFreeHashTable() {}

  void Dump() {
    Node* p = GetBucketHeadByIndex(0);
    while (p) {
      LOG("%p,dummy=%d,hash=%lu,-->", p, p->IsDummy(), p->hash);
      p = p->next;
    }
    LOG("%s", "\n");
  }

  void Insert(const K& key, const V& value) {
    RegularNode* new_node = new RegularNode(key, value, hash_func_);
    DummyNode* head = GetBucketHeadByHash(new_node->hash);
    InsertRegularNode(head, new_node);
  }

  void Insert(K&& key, const V& value) {
    RegularNode* new_node = new RegularNode(std::move(key), value, hash_func_);
    DummyNode* head = GetBucketHeadByHash(new_node->hash);
    InsertRegularNode(head, new_node);
  }

  void Insert(const K& key, V&& value) {
    RegularNode* new_node = new RegularNode(key, std::move(value), hash_func_);
    DummyNode* head = GetBucketHeadByHash(new_node->hash);
    InsertRegularNode(head, new_node);
  }

  void Insert(K&& key, V&& value) {
    RegularNode* new_node =
        new RegularNode(std::move(key), std::move(value), hash_func_);
    DummyNode* head = GetBucketHeadByHash(new_node->hash);
    InsertRegularNode(head, new_node);
  }

  bool Delete(const K& key) {
    HashKey hash = hash_func_(key);
    DummyNode* head = GetBucketHeadByHash(hash);
    RegularNode delete_node(key, hash_func_);
    return DeleteNode(head, &delete_node);
  }

  bool Find(const K& key, V& value) {
    HashKey hash = hash_func_(key);
    DummyNode* head = GetBucketHeadByHash(hash);
    RegularNode find_node(key, hash_func_);
    return FindNode(head, &find_node, value);
  };

  size_t size() const { return size_.load(std::memory_order_acquire); }
  size_t bucket_size() const {
    return 1 << power_of_2_.load(std::memory_order_acquire);
  }

 private:
  Segment* NewSegments(int level) {
    Segment* segments = new Segment[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      segments[i].level = level;
      segments[i].data.store(nullptr, std::memory_order_release);
    }
    return segments;
  }

  Bucket* NewBuckets() {
    Bucket* buckets = new Bucket[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      buckets[i].store(nullptr, std::memory_order_release);
    }
    return buckets;
  }

  // Initialize bucket recursively.
  DummyNode* InitializeBucket(BucketIndex bucket_index);

  // When the table size is 2^i , a logical table bucket b contains items whose
  // keys k maintain k mod 2^i = b. When the size becomes 2^i+1, the items of
  // this bucket are split into two buckets: some remain in the bucket b, and
  // others, for which k mod 2^(i+1) == b + 2^i.
  BucketIndex GetBucketParent(BucketIndex bucket_index) const {
    //__builtin_clzl: Get number of leading zero bits.
    // Unset the MSB(most significant bit) of bucket_index;
    return (~(0x8000000000000000 >> (__builtin_clzl(bucket_index))) &
            bucket_index);
  };

  // Get the head node of bucket, if bucket not exist then return nullptr or
  // return head.
  DummyNode* GetBucketHeadByIndex(BucketIndex bucket_index);

  // Get the head node of bucket, if bucket not exist then initialize it and
  // return head.
  DummyNode* GetBucketHeadByHash(HashKey hash) {
    BucketIndex bucket_index = hash % bucket_size();
    DummyNode* head = GetBucketHeadByIndex(bucket_index);
    if (nullptr == head) {
      head = InitializeBucket(bucket_index);
    }
    return head;
  }

  // Harris' OrderedListBasedset with Michael's hazard pointer to manage memory,
  // See also https://github.com/bhhbazinga/LockFreeLinkedList.
  void InsertRegularNode(DummyNode* head, RegularNode* new_node);
  bool InsertDummyNode(DummyNode* head, DummyNode* new_node,
                       DummyNode** real_head);
  bool DeleteNode(DummyNode* head, Node* delete_node);
  bool FindNode(DummyNode* head, RegularNode* find_node, V& value) {
    Node* prev;
    Node* cur;
    bool found = SearchNode(head, find_node, &prev, &cur);
    if (found) {
      value = *static_cast<RegularNode*>(cur)->value.load(
          std::memory_order_acquire);
    }
    ClearHazardPointer();
    return found;
  }

  // Traverse list begin with head until encounter nullptr or the first node
  // which is greater than or equals to the given search_node.
  bool SearchNode(DummyNode* head, Node* search_node, Node** prev_ptr,
                  Node** cur_ptr);

  // Compare two nodes according to their reverse_hash and the key.
  bool Less(Node* node1, Node* node2) const {
    if (node1->reverse_hash != node2->reverse_hash) {
      return node1->reverse_hash < node2->reverse_hash;
    }

    if (node1->IsDummy() || node2->IsDummy()) {
      // When initialize bucket currently, that could happen.
      assert(node1->IsDummy() && node2->IsDummy());
      return false;
    }

    return static_cast<RegularNode*>(node1)->key <
           static_cast<RegularNode*>(node2)->key;
  }

  bool GreaterOrEquals(Node* node1, Node* node2) const {
    return !(Less(node1, node2));
  }

  bool Equals(Node* node1, Node* node2) const {
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
    Node(HashKey hash_, bool dummy)
        : hash(hash_),
          reverse_hash(dummy ? DummyKey(hash) : RegularKey(hash)),
          next(nullptr) {}

    virtual ~Node() {}

    HashKey Reverse(HashKey hash) const {
      return reverse8bits_[hash & 0xff] << 56 |
             reverse8bits_[(hash >> 8) & 0xff] << 48 |
             reverse8bits_[(hash >> 16) & 0xff] << 40 |
             reverse8bits_[(hash >> 24) & 0xff] << 32 |
             reverse8bits_[(hash >> 32) & 0xff] << 24 |
             reverse8bits_[(hash >> 40) & 0xff] << 16 |
             reverse8bits_[(hash >> 48) & 0xff] << 8 |
             reverse8bits_[(hash >> 56) & 0xff];
    }
    HashKey RegularKey(HashKey hash) const {
      return Reverse(hash | 0x8000000000000000);
    }
    HashKey DummyKey(HashKey hash) const { return Reverse(hash); }

    void Dump() const {
      LOG("%p,is_dummy=%d,hash=%lu,reverse_hash=%lu,\n", this, IsDummy(), hash,
          reverse_hash);
    }

    virtual bool IsDummy() const { return (reverse_hash & 0x1) == 0; }
    Node* get_next() const { return next.load(std::memory_order_acquire); }

    const HashKey hash;
    const HashKey reverse_hash;
    std::atomic<Node*> next;
  };

  // Head node of bucket
  struct DummyNode : Node {
    DummyNode(BucketIndex bucket_index) : Node(bucket_index, true) {}
    ~DummyNode() override {}

    bool IsDummy() const override { return true; }
  };

  struct RegularNode : Node {
    RegularNode(const K& key_, const V& value_, const Hash& hash_func)
        : Node(hash_func(key_), false), key(key_), value(new V(value_)) {}
    RegularNode(const K& key_, V&& value_, const Hash& hash_func)
        : Node(hash_func(key_), false),
          key(key_),
          value(new V(std::move(value_))) {}
    RegularNode(K&& key_, const V& value_, const Hash& hash_func)
        : Node(hash_func(key_), false),
          key(std::move(key_)),
          value(new V(value_)) {}
    RegularNode(K&& key_, V&& value_, const Hash& hash_func)
        : Node(hash_func(key_), false),
          key(std::move(key_)),
          value(new V(std::move(value_))) {}

    RegularNode(const K& key_, const Hash& hash_func)
        : Node(hash_func(key_), false), key(key_), value(nullptr) {}

    ~RegularNode() override {
      V* ptr = value.load(std::memory_order_acquire);
      delete ptr;
    }

    bool IsDummy() const override { return false; }

    const K key;
    std::atomic<V*> value;
  };

  struct Segment {
    Segment() : level(1), data(nullptr) {}
    explicit Segment(int level_) : level(level_), data(nullptr) {}

    Bucket* get_sub_buckets() const {
      return static_cast<Bucket*>(data.load(std::memory_order_acquire));
    }

    Segment* get_sub_segments() const {
      return static_cast<Segment*>(data.load(std::memory_order_acquire));
    }

    ~Segment() {
      // void* ptr = data.load(std::memory_order_acquire);
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

  std::atomic<size_t> power_of_2_;   // bucket size == 2^power_of_2_
  std::atomic<size_t> size_;         // item size
  Hash hash_func_;                   // hash function
  Segment segments_[kSegmentSize];   // top level sengments
  static size_t reverse8bits_[256];  // lookup table for reverse bits quickly
};

// Fast reverse bits using Lookup Table.
#define R2(n) n, n + 2 * 64, n + 1 * 64, n + 3 * 64
#define R4(n) R2(n), R2(n + 2 * 16), R2(n + 1 * 16), R2(n + 3 * 16)
#define R6(n) R4(n), R4(n + 2 * 4), R4(n + 1 * 4), R4(n + 3 * 4)
// Lookup Table that store the reverse of each 8bit number.
template <typename K, typename V, typename Hash>
size_t LockFreeHashTable<K, V, Hash>::reverse8bits_[256] = {R6(0), R6(2), R6(1),
                                                            R6(3)};

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::DummyNode*
LockFreeHashTable<K, V, Hash>::InitializeBucket(BucketIndex bucket_index) {
  BucketIndex parent_index = GetBucketParent(bucket_index);
  DummyNode* parent_head = GetBucketHeadByIndex(parent_index);
  if (nullptr == parent_head) {
    parent_head = InitializeBucket(parent_index);
  }

  int level = 1;
  SegmentIndex segment_index = bucket_index;
  Segment* segments = segments_;  // Point to current segment.
  while (level++ <= kMaxLevel - 2) {
    segment_index /= kSegmentSize;
    Segment& cur_segment = segments[segment_index];
    Segment* sub_segments = cur_segment.get_sub_segments();
    if (nullptr == sub_segments) {
      // Try allocate segments.
      sub_segments = NewSegments(level);
      void* expected = nullptr;
      if (!cur_segment.data.compare_exchange_strong(
              expected, sub_segments, std::memory_order_release)) {
        delete[] sub_segments;
        sub_segments = static_cast<Segment*>(expected);
      }
    }
    segments = sub_segments;
  }

  segment_index /= kSegmentSize;
  Segment& cur_segment = segments[segment_index];
  Bucket* buckets = cur_segment.get_sub_buckets();
  if (nullptr == buckets) {
    // Try allocate buckets.
    void* expected = nullptr;
    buckets = NewBuckets();
    if (!cur_segment.data.compare_exchange_strong(expected, buckets,
                                                  std::memory_order_release)) {
      delete[] buckets;
      buckets = static_cast<Bucket*>(expected);
    }
  }

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  DummyNode* head = bucket.load(std::memory_order_acquire);
  if (nullptr == head) {
    // Try allocate dummy head.
    head = new DummyNode(bucket_index);
    DummyNode* real_head;  // If insert failed, real_head is the head of bucket.
    assert((parent_head->reverse_hash & 0x1) == 0);
    if (InsertDummyNode(parent_head, head, &real_head)) {
      // Dummy head must be inserted into the list before storing into bucket.
      assert(bucket.load(std::memory_order_acquire) == nullptr);
      bucket.store(head, std::memory_order_release);
    } else {
      delete head;
      assert((real_head->reverse_hash & 0x1) == 0);
      assert(bucket_index == real_head->hash);
      head = real_head;
    }
  }
  return head;
}

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::DummyNode*
LockFreeHashTable<K, V, Hash>::GetBucketHeadByIndex(BucketIndex bucket_index) {
  int level = 1;
  SegmentIndex segment_index = bucket_index;
  const Segment* segments = segments_;
  while (level++ <= kMaxLevel - 2) {
    segment_index /= kSegmentSize;
    segments = segments[segment_index].get_sub_segments();
    if (nullptr == segments) return nullptr;
  }

  segment_index /= kSegmentSize;
  Bucket* buckets = segments[segment_index].get_sub_buckets();
  if (nullptr == buckets) return nullptr;

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  return bucket.load(std::memory_order_acquire);
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::InsertDummyNode(DummyNode* parent_head,
                                                    DummyNode* new_head,
                                                    DummyNode** real_head) {
  Node* prev;
  Node* cur;
  do {
    if (SearchNode(parent_head, new_head, &prev, &cur)) {
      // The head of bucket already insert into list.
      assert((cur->reverse_hash & 0x1) == 0);
      *real_head = static_cast<DummyNode*>(cur);
      ClearHazardPointer();
      return false;
    }
    new_head->next.store(cur, std::memory_order_release);
  } while (!prev->next.compare_exchange_weak(
      cur, new_head, std::memory_order_release, std::memory_order_acquire));
  ClearHazardPointer();
  return true;
}

template <typename K, typename V, typename Hash>
void LockFreeHashTable<K, V, Hash>::InsertRegularNode(DummyNode* head,
                                                      RegularNode* new_node) {
  Node* prev;
  Node* cur;
  assert((head->reverse_hash & 0x1) == 0);
  do {
    if (SearchNode(head, new_node, &prev, &cur)) {
      assert(!(new_node->reverse_hash & 0x1) == 0);
      assert(!(cur->reverse_hash & 0x1) == 0);
      V* new_value = new_node->value.load(std::memory_order_acquire);
      V* old_value = static_cast<RegularNode*>(cur)->value.exchange(
          new_value, std::memory_order_release);
      delete old_value;
      new_node->value.store(nullptr, std::memory_order_release);
      delete new_node;
      ClearHazardPointer();
      return;
    }
    new_node->next.store(cur, std::memory_order_release);
  } while (!prev->next.compare_exchange_weak(
      cur, new_node, std::memory_order_release, std::memory_order_acquire));
  ClearHazardPointer();

  if (size_ == 5) {
    head->Dump();
    new_node->Dump();
    Dump();
    assert(false);
  }

  size_t size = size_.fetch_add(1, std::memory_order_release) + 1;
  size_t power = power_of_2_.load(std::memory_order_acquire);
  if ((1 << power) * kLoadFactor < size) {
    power_of_2_.compare_exchange_strong(power, power + 1,
                                        std::memory_order_release);
  }
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::SearchNode(DummyNode* head,
                                               Node* search_node,
                                               Node** prev_ptr,
                                               Node** cur_ptr) {
  Reclaimer& reclaimer = Reclaimer::GetInstance();
try_again:
  Node* prev = head;
  Node* cur = prev->get_next();
  Node* next;
  while (true) {
    reclaimer.MarkHazard(0, cur);
    // Make sure prev is the predecessor of cur,
    // so that cur is properly marked as hazard.
    if (prev->get_next() != cur) goto try_again;

    if (nullptr == cur) {
      *prev_ptr = prev;
      *cur_ptr = cur;
      return false;
    }

    next = cur->get_next();
    if (is_marked_reference(next)) {
      if (!prev->next.compare_exchange_strong(cur,
                                              get_unmarked_reference(next)))
        goto try_again;

      reclaimer.ReclaimLater(cur, LockFreeHashTable<K, V, Hash>::OnDeleteNode);
      reclaimer.ReclaimNoHazardPointer();
      size_.fetch_sub(1, std::memory_order_release);
      cur = get_unmarked_reference(next);
    } else {
      if (prev->get_next() != cur) goto try_again;

      // Can not get copy_cur after above invocation,
      // because prev may not be the predecessor of cur at this point.
      if (GreaterOrEquals(cur, search_node)) {
        *prev_ptr = prev;
        *cur_ptr = cur;
        return Equals(cur, search_node);
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
bool LockFreeHashTable<K, V, Hash>::DeleteNode(DummyNode* head,
                                               Node* delete_node) {
  Node* prev;
  Node* cur;
  Node* next;
  do {
    do {
      if (!SearchNode(head, delete_node, &prev, &cur)) {
        ClearHazardPointer();
        return false;
      }
      next = cur->get_next();
    } while (is_marked_reference(next));
    // Logically delete cur by marking cur->next.
  } while (!cur->next.compare_exchange_weak(next, get_marked_reference(next),
                                            std::memory_order_release,
                                            std::memory_order_acquire));

  if (prev->next.compare_exchange_strong(cur, next, std::memory_order_release,
                                         std::memory_order_acquire)) {
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