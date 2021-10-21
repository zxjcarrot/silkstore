1. Silkstore has defined the leaf_index_path in Options, but open leafindex maybe cause segment fault when a leafindex exists.

2. The Recovery function need to be rewritten to ensure silkstore's crash consistency.

3. There may be some bugs in silkstore's method of merging imms into a bigger imm. Especially in the multi-threaded read-write environment, there will be synchronization problems


4.The last insertion may exceed the space size of the nvmemtable. MakeRoomForWrite() can only compare memtbl_size and memtable_capacity_. If the new data ready to insert into nvmemtable exceeds memtable_capacity_ - memtbl_size, an error will occur.

5. The key stored by nvmemtable has no sequence number, which will result in an error when calling Seek() in the iterator of silkstore.