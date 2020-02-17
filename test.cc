#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "lockfree_hashtable.h"

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;

  struct Hash {
    size_t operator()(int x) const { return x; }
  };

  // /* code */
  // LockFreeHashTable<int, int, Hash> ht;
  // assert(ht.bucket_size() == 2);
  // ht.Insert(1, 1);
  // assert(ht.bucket_size() == 2);
  // ht.Dump();
  // ht.Insert(2, 2);
  // assert(ht.bucket_size() == 4);
  // ht.Dump();
  // ht.Insert(3, 3);
  // ht.Dump();
  // assert(ht.bucket_size() == 8);
  // ht.Insert(4, 4);
  // ht.Dump();
  // assert(ht.bucket_size() == 8);
  // ht.Insert(5, 5);
  // ht.Dump();
  // assert(ht.bucket_size() == 16);
  // ht.Insert(5, 50);
  // assert(ht.size() == 5);

  // int x;
  // assert(ht.Find(1, x) && x == 1);
  // assert(ht.Find(2, x) && x == 2);
  // assert(ht.Find(3, x) && x == 3);
  // assert(ht.Find(4, x) && x == 4);
  // assert(ht.Find(5, x) && x == 50);

  while (true) {
    // LOG("-----------------------------%s","\n");
    LockFreeHashTable<int, int, Hash> ht;
    std::vector<std::thread> threads;
    std::atomic<bool> start = false;
    int n = 2;
    for (int i = 0; i < 2; ++i) {
      threads.push_back(std::thread([&] {
        while (!start) {
          std::this_thread::yield();
        }
        for (int j = 0; j < n; ++j) {
          ht.Insert(j, j);
        }
      }));
    }

    start = true;

    for (auto& t : threads) {
      t.join();
    }

    // ht.Dump();
    // LOG("sz=%d\n", ht.size());
    assert(ht.size() == n);
    // LOG(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>%s","\n");
  }

  return 0;
}
