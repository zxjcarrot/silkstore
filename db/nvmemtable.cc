// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/nvmemtable.h"
#include <iostream>

namespace leveldb {


static Slice NvmGetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;

  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted

  return Slice(p, len);
}



// helper function used to init MagicNum
void initMagicNum(char* magicNum){
    magicNum[0] = (char) (0xCA);  
    magicNum[1] = (char) (0xFE);  
    magicNum[2] = (char) (0xBA);  
    magicNum[3] = (char) (0xBE);  
}

NvmemTable::NvmemTable(const InternalKeyComparator& cmp, DynamicFilter * dynamic_filter, silkstore::Nvmem* nvmem )
    : comparator_(cmp),
      refs_(0),
      num_entries_(0),
      searches_(0),
      dynamic_filter(dynamic_filter),
      nvmem(nvmem),
      memory_usage_(0) {
        initMagicNum(buf);
      } 

NvmemTable::~NvmemTable() {
  assert(refs_ == 0);
  if (dynamic_filter) {
    delete dynamic_filter;
    dynamic_filter = nullptr;
  }
  if (nvmem){
    delete nvmem;
    nvmem = nullptr;
  } 
}

size_t NvmemTable::Searches() const { return searches_; }
size_t NvmemTable::NumEntries() const { return num_entries_; }
size_t NvmemTable::ApproximateMemoryUsage() { return  memory_usage_; } //arena_.MemoryUsage(); }

int NvmemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = NvmGetLengthPrefixedSlice(aptr);
  Slice b = NvmGetLengthPrefixedSlice(bptr);   
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class NvmemTableIterator: public Iterator {
 public:
  explicit NvmemTableIterator(NvmemTable::Index* index) : index(index) { 
    iter_ = index->begin();
  }
  virtual bool Valid() const { return iter_ != index->end() && iter_->second.first > 0 ; }
  virtual void Seek(const Slice& k) { iter_ = index->find(k.ToString()); }
  virtual void SeekToFirst() { iter_ = index->begin(); }
  virtual void SeekToLast() {  
      fprintf(stderr, "MemTableIterator's SeekToLast() is not implemented !");  
      assert(true);
  }
  virtual void Next() {  ++iter_; }
  virtual void Prev() { --iter_;
    /* iter_.Prev(); */ 
      fprintf(stderr, "MemTableIterator's Prev() is not implemented ! \n");  
      sleep(111);
      assert(true);
  }
  virtual Slice key() const {  
     return NvmGetLengthPrefixedSlice((char *)(iter_->second.first + 4)); 
  }
  virtual Slice value() const {
    Slice key_slice = NvmGetLengthPrefixedSlice((char *)(iter_->second.first + 4));
    return NvmGetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  NvmemTable::Index* index;
  NvmemTable::Index::iterator iter_;

  // No copying allowed
  NvmemTableIterator(const NvmemTableIterator&);
  void operator=(const NvmemTableIterator&);
};

Iterator* NvmemTable::NewIterator() {
  return new NvmemTableIterator(&index_);
}

void NvmemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  magic number
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  
  // first, insert magic number used to recover data 
  char* p = EncodeVarint32(buf + 4, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + 4 + encoded_len);

// If kv is already exits in memtable, try to update it in place 
 if (dynamic_filter && dynamic_filter->KeyMayMatch(key)){
   // Due to bloom filter exits positive false, if kv can it find in memtable 
   // and it's original log size greater than the new log size update it in place    
    if (index_.count(key.ToString()) ){
        auto AddSize =  index_[key.ToString()];
        if(AddSize.second >=  encoded_len + 4){
          nvmem->update(AddSize.first, buf, encoded_len + 4);
          return ;
        }
    }
 } 
   
 //if can't update log in place, append it to the tail
  uint64_t address = nvmem->insert(buf, encoded_len + 4);
  index_[key.ToString()] = std::make_pair(address,encoded_len + 4);
  if (dynamic_filter)
    dynamic_filter->Add(key);
  ++num_entries_;
  // update memory_usage_ to recode nvm's usage size
  memory_usage_ += encoded_len + 4;
}

bool NvmemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  if (dynamic_filter && !dynamic_filter->KeyMayMatch(key.user_key()))
    return false;

  ++searches_;
  Slice memkey = key.user_key();

  bool suc = index_.count(memkey.ToString()) ;
  
  if (suc) {
    uint64_t address =  index_[memkey.ToString()].first;
    // entry format is:
    //    magicNum
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr((char *) (address + 4), 
        (char *) (address + 9), &key_length);  // +4: the first 4 bits is magic number 
                                              //  +9: we assume "p" is not corrupted
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = NvmGetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());     
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice("Deleted !"));
          return true;
      }
    }
  }
  *s = Status::NotFound(Slice());
  return false;
}
}  // namespace leveldb
