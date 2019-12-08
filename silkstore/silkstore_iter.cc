//
// Created by zxjcarrot on 2019-07-24.
//

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "silkstore/silkstore_iter.h"

namespace leveldb {

namespace silkstore {

Iterator *NewDBIterator(
        const Comparator *user_key_comparator,
        Iterator *internal_iter,
        SequenceNumber sequence) {
    return new DBIter(user_key_comparator, internal_iter, sequence);
}

}  // namespace silkstore
}  // namespace leveldb
