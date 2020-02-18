# LockFreeHashTable
Lock Free Resizable Hash Table Based On Split-Ordered Lists.
## Feature
  * Thread-safe and Lock-free.
  * ABA safe.
  * Support Multi-producer & Multi-consumer.
  * Use Hazard Pointer to manage memory.
  * Lock Free LinkedList base on Harris' ListBasedSet, see also [LockFreeLinkedList](https://github.com/bhhbazinga/LockFreeLinkedList)
  * Resize without waiting.
## Benchmark
  Magnitude     | Insert      | Find       | Delete     | Insert&Find&Delete|
  :-----------  | :-----------| :----------|:-----------| :-----------------
  10K           | 7.8ms       | 1.6ms      | 2.1ms      | 10.4ms
  100K          | 74.9ms      | 13.4ms     | 18.4ms     | 111.2ms
  1000K         | 961ms       | 117.8ms    | 216.1ms    | 1264.1ms
  
The above data was tested on my 2013 macbook-pro with Intel Core i7 4 cores 2.3 GHz.

The data of first three column was obtained by starting 8 threads to insert concurrently, find concurrently, delete concurrently, the data of four column was obtained by starting 2 threads to insert, 2 threads to find, 2 threads to delete concurrently, each looped 10 times to calculate the average time consumption.
See also [test](test.cc).
## Build
```
make && ./test
```
## API
```C++
bool Insert(const K& key, const V& value);
bool Insert(const K& key, V&& value);
bool Insert(K&& key, const V& value);
bool Insert(K&& key, V&& value);
bool Find(const K& key, V& value);
bool Delete(const T& data);
size_t size() const;
```
## TODO List
- [ ] Shrink Hash Table without waiting.
## Reference
[1]A Pragmatic Implementation of Non-BlockingLinked-Lists. Timothy L.Harris\
[2]Hazard Pointers: Safe Memory Reclamation for Lock-Free Objects. Maged M. Michael\
[3]Split-Ordered Lists: Lock-Free Extensible Hash Tables. Tel-Aviv University and Sun Microsystems Laboratories, Tel-Aviv, Israel
