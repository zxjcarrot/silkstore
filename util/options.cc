// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#ifndef MEM_CAPACITY
#define MEM_CAPACITY (128* (1UL<<32))
#endif
const size_t kSegmentFileSizeThreshold = 64*1024*1024;
const size_t kStorageBlocKSize = 4096;
namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(nullptr),
      write_buffer_size(4<<20),
      max_memtbl_capacity(MEM_CAPACITY >> 1),
      segment_file_size_thresh(kSegmentFileSizeThreshold),
      leaf_max_num_miniruns(15),
      storage_block_size(kStorageBlocKSize),
      memtbl_to_L0_ratio(30),
      max_open_files(1000),
      block_cache(nullptr),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      filter_policy(nullptr)
      {}

}  // namespace leveldb
