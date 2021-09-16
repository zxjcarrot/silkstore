// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/nvmemtable.h"
#include "leveldb/nvm_write_batch.h"
#include "leveldb/write_batch.h"

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

NvmemTable::NvmemTable(const InternalKeyComparator& cmp, DynamicFilter * dynamic_filter, 
    silkstore::Nvmem* nvmem, silkstore::NvmLog* nvmlog )
    : comparator_(cmp),
      refs_(0),
      num_entries_(0),
      searches_(0),
      dynamic_filter(dynamic_filter),
      nvmem(nvmem),
      nvmlog(nvmlog),      
      memory_usage_(0) {
        initMagicNum(magicNum);
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
    iter_ = index->begin_unsafe();
  }
  virtual bool Valid() const { return iter_.valid() ; }
  virtual void Seek(const Slice& k) { iter_ = index->seek(k.ToString()); }
  virtual void SeekToFirst() { iter_ = index->begin_unsafe(); }
  virtual void SeekToLast() {  
      fprintf(stderr, "MemTableIterator's SeekToLast() is not implemented !");  
      assert(true);
  }
  virtual void Next() {  ++iter_; }
  virtual void Prev() { 
    /* iter_.Prev(); */ 
      fprintf(stderr, "MemTableIterator's Prev() is not implemented ! \n");  
      sleep(111);
      assert(true);
  }
  virtual Slice key() const {  
     return NvmGetLengthPrefixedSlice((char *)(iter_.value())); 
  }
  virtual Slice value() const {
    Slice key_slice = NvmGetLengthPrefixedSlice((char *)(iter_.value()));
    return NvmGetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }


  virtual Status status() const { return Status::OK(); }

 private:
  NvmemTable::Index* index;
  NvmemTable::Index::unsafe_iterator iter_;

  // No copying allowed
  NvmemTableIterator(const NvmemTableIterator&);
  void operator=(const NvmemTableIterator&);
};

Iterator* NvmemTable::NewIterator() {
  return new NvmemTableIterator(&index_);
}

IndexIterator NvmemTable::NewIndexIterator() {
  return index_.begin_unsafe();
}


Status NvmemTable::AddBatch(const NvmWriteBatch* batch){
   int64_t offset = nvmem->insert(batch->buf, batch->offset_); 
  //std::cout<< "batch size :" << batch->offset_<< "\n";
  for(int i = 0; i < batch->offset_arr_.size(); i++){
      uint64_t address = offset + batch->offset_arr_[i].second;
      index_.insert(batch->offset_arr_[i].first, address);
      if (dynamic_filter)
        dynamic_filter->Add(batch->offset_arr_[i].first);
  }
  memory_usage_ += batch->offset_;
  return Status::OK();
}

bool NvmemTable::AddIndex(std::string key ,uint64_t val){
    index_.insert(key,val);
    return true;
}




Status NvmemTable::AddBatch(const WriteBatch* batch){
 // const char* add = ;
  /* int64_t offset = nvmem->insert(batch->StrAddress() , batch->StrSize()); 
  //std::cout<< "batch size :" << batch->offset_<< "\n";
  for(int i = 0; i < batch->offset_arr_.size(); i++){
      uint64_t address = offset + batch->offset_arr_[i].second;
      index_.insert(batch->offset_arr_[i].first, address);
      if (dynamic_filter)
        dynamic_filter->Add(batch->offset_arr_[i].first);
  } */
  return Status::OK();
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
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len); 
  memcpy(buf + encoded_len, magicNum, 4);
  uint64_t address = nvmem->insert(buf, encoded_len + 4);
  index_.insert(key.ToString(), address);
  // nvmlog->append(address);

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
  
  uint64_t address = -1;

  bool suc = index_.lookup(memkey.ToString(), address);
  
  if (suc) {
   /*  Slice foundkey = NvmGetLengthPrefixedSlice((char *)(address));
    std::cout << "found ! key: "<< foundkey.ToString() <<"\n"; */
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
    const char* key_ptr = GetVarint32Ptr((char *) (address), 
        (char *) (address + 5), &key_length);  // 
                                              //  +5: we assume "p" is not corrupted
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
    }else{
      std::cout << " Fuck seq !";
    }
  }
  *s = Status::NotFound(Slice());
  return false;
}
}  // namespace leveldb
