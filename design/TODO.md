1. I have define the leaf_index_path in Options, but open leafindex maybe cause segment fault when a leafindex exists.

2. The Recovery function need to be rewritten to ensure silkstore's crash consistency.

3. There may be some bugs in silkstore's method of merging imms into a bigger imm. Especially in the multi-threaded read-write environment, there will be synchronization problems