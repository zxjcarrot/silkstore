// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// NvmWriteBatch::rep_ :=
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
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {
// NvmWriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;
NvmWriteBatch::NvmWriteBatch() {
  Clear();
}

NvmWriteBatch::~NvmWriteBatch() { }

void NvmWriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}
size_t NvmWriteBatch::ApproximateSize() const {
  return rep_.size();
}
int Count(const NvmWriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}
void SetCount(NvmWriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}
SequenceNumber Sequence(const NvmWriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}
void SetSequence(NvmWriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}
void NvmWriteBatch::Put(const Slice& key, const Slice& value) {
  SetCount(this, Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}
void NvmWriteBatch::Delete(const Slice& key) {
  SetCount(this,Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}
void AppendImp(NvmWriteBatch* dst, const NvmWriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

void NvmWriteBatch::Append(const NvmWriteBatch *source) {
  AppendImp(this, source);
}

void SetContents(NvmWriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

}  // namespace leveldb
