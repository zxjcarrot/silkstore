// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/nvm_write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"



namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

NvmWriteBatch::NvmWriteBatch() {
  Clear();
}

NvmWriteBatch::~NvmWriteBatch() { }

void NvmWriteBatch::Clear() {
  offset_ = 0;
  counter_ = 0;
}

size_t NvmWriteBatch::ApproximateSize() const {
  return offset_;
}


Slice NvmGetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;

  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted

  return Slice(p, len);
}


void NvmWriteBatch::Put(const Slice& key, const Slice& value) {
 // WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* p = EncodeVarint32(buf + offset_, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq_ << 8) | kTypeValue);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len); 




  //auto pair = std::make_pair(key.ToString(), offset_);
  // offset_arr_.push_back(std::make_pair(key.ToString(), offset_));
  counter_++;

 /*  Slice batchkey = NvmGetLengthPrefixedSlice(buf + offset_);
  Slice batchvalue = NvmGetLengthPrefixedSlice(batchkey.data() + batchkey.size());
  std::cout << "batch insert Key: " << batchkey.ToString() << 
       " value " << batchvalue.ToString() << "\n";
 */

  offset_ += encoded_len;
  seq_++;
}

void NvmWriteBatch::Delete(const Slice& key) {
  Slice value = Slice("");
 // WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* p = EncodeVarint32(buf + offset_, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq_ << 8) | kTypeDeletion);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len); 

  offset_arr_.push_back(std::make_pair(key.ToString(), offset_));

//offset_arr_.push_back(offset_);
  counter_ ++;
  offset_ += encoded_len;
  seq_++;
}

void NvmWriteBatch::Append(const NvmWriteBatch* source) {
    memcpy(buf + offset_, source->buf, source->offset_);
    for(int i = 0; i < source->offset_arr_.size(); i++){
      offset_arr_.push_back(std::make_pair( 
        source->offset_arr_[i].first, source->offset_arr_[i].second + offset_
      ));
    }
    counter_ += source->counter_;
    offset_ += source->offset_;
}




/* Status WriteBatchInternal::InsertInto(const NvmWriteBatch* b, NvmemTable* memtable){
 
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
} */



}  // namespace leveldb
