#include <functional>
#include <string>

#include "lockfree_hashtable.h"

int main(int argc, char const* argv[]) {
  (void)argc;
  (void)argv;
  struct Hash {
    size_t operator()(int x) const { return x; }
  };

  /* code */
  LockFreeHashTable<int, int, Hash> ht;
  assert(ht.bucket_size() == 2);
  ht.Insert(1, 1);
  assert(ht.bucket_size() == 2);
  ht.Dump();
  ht.Insert(2, 2);
  assert(ht.bucket_size() == 4);
  ht.Dump();
  ht.Insert(3, 3);
  ht.Dump();
  assert(ht.bucket_size() == 8);
  ht.Insert(4, 4);
  ht.Dump();
  // assert(ht.bucket_size() == 8);


  // ht.Insert(5, 5);
  // assert(ht.bucket_size() == 16);
  // ht.Insert(6, 6);
  // assert(ht.bucket_size() == 16);
  // ht.Dump();

  // ht.Insert("d", 4);
  // ht.Insert("e", 5);
  // assert(ht.bucket_size() == 16);

  int x;
  // assert(ht.Find("a", x) && x == 1);
  // assert(ht.Find("b", x) && x == 2);
  // assert(!ht.Find("c", x));
  // assert(ht.Find("e", x) && x == 5);

  // ht.Delete(2);
  // assert(ht.Find(1, x) && x == 1);
  bool flag = ht.Find(2, x);
  ht.Dump();
  assert(flag);
  // assert(x == 2);

  // ht.Dump();
  // assert(ht.size() == 4);
  // assert(!ht.Find("a", x));
  // assert(ht.Find("b", x) && x == 2);

  return 0;
}
