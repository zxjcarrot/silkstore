// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_NVM_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_NVM_WRITE_BATCH_H_

#include <string>
#include "leveldb/export.h"
#include "leveldb/status.h"
#include "db/dbformat.h"
#include <vector>

// #include "db/nvmemtable.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT NvmWriteBatch {
 public:
  NvmWriteBatch();

  // Intentionally copyable.
  NvmWriteBatch(const NvmWriteBatch&) = default;
  NvmWriteBatch& operator =(const NvmWriteBatch&) = default;

  ~NvmWriteBatch();


  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  size_t ApproximateSize() const;

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  void Append(const NvmWriteBatch* source);

// private:
  friend class WriteBatchInternal;
  char buf[16*1024];
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
  uint64_t offset_;
  int counter_;
  SequenceNumber seq_;
  std::vector<std::pair<std::string, uint64_t> >offset_arr_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
