/**
 * @ Author: Yunxiao Du
 * @ Create Time: 2021-05-20 20:19:30
 * @ Description: Using nvm and dram to replace leveldb's oringinal memtable
 */

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_NVMEMTABLE_H_
#define STORAGE_LEVELDB_DB_NVMEMTABLE_H_


#include <string>
#include <map>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "leveldb/filter_policy.h"

#include "nvm/nvmem.h"
#include "nvm/nvmlog.h"
#include "nvm/btree.h"

namespace leveldb {


class InternalKeyComparator;
class MemTableIterator;

class NvmemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit NvmemTable(const InternalKeyComparator& comparator, 
      DynamicFilter * dynamic_filter, silkstore::Nvmem *nvmem, silkstore::NvmLog *nvmlog);


 // explicit NvmemTable(const InternalKeyComparator& comparator, 
 //     DynamicFilter * dynamic_filter, silkstore::Nvmem *nvmem);


  // Increase reference count.
  void Ref() { ++refs_; }

  void print(){
    nvmem->print();
  }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

  size_t NumEntries() const;
  size_t Searches() const;
private:
  ~NvmemTable();  // Private since only Unref() should be used to delete it
// private:
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class NvmemTableIterator;
  friend class NvmemTableBackwardIterator;


 typedef leveldb::silkstore::BTree<std::string,uint64_t> Index;
//  typedef std::map<std::string ,uint64_t > ;

  KeyComparator comparator_;
  int refs_;
  Index index_;
  silkstore::Nvmem *nvmem;
  silkstore::NvmLog *nvmlog;
  // buf's first 4 bytes magic number 0xCAFEBABE used to recover data 
  char buf[5120];
  char magicNum[4];
  size_t num_entries_;
  size_t searches_;
  size_t memory_usage_;
  DynamicFilter * dynamic_filter;
  // No copying allowed
  NvmemTable(const NvmemTable&);
  void operator=(const NvmemTable&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
