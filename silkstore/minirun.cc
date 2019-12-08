//
// Created by zxjcarrot on 2019-06-29.
//

#include <string>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "table/block_builder.h"

#include "silkstore/segment.h"
#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

MiniRun::MiniRun(const Options *options, RandomAccessFile *file,
                 uint64_t off, uint64_t size, Block &index_block)
        : options(options),
          file(file),
          run_start_off(off),
          run_size(size),
          index_block(index_block) {}

static void DeleteBlock(void *arg, void *ignored) {
    delete reinterpret_cast<Block *>(arg);
}

static void DeleteCachedBlock(const Slice &key, void *value) {
    Block *block = reinterpret_cast<Block *>(value);
    delete block;
}

static void ReleaseBlock(void *arg, void *h) {
    Cache *cache = reinterpret_cast<Cache *>(arg);
    Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
    cache->Release(handle);
}

Iterator *MiniRun::NewIteratorForOneBlock(const leveldb::ReadOptions &read_options, BlockHandle handle) {
    Block *block = nullptr;

    BlockContents contents;

    Status s = ReadBlock(this->file, read_options, handle, &contents);
    if (s.ok()) {
        block = new Block(contents);
    }

    Iterator *iter;
    if (block == nullptr) {
        return NewErrorIterator(s);
    } else {
        iter = block->NewIterator(this->options->comparator);
        iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    }
    return iter;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *MiniRun::BlockReader(void *arg,
                               const ReadOptions &options,
                               const Slice &index_value) {
    MiniRun *run = reinterpret_cast<MiniRun *>(arg);
    Cache *block_cache = run->options->block_cache;
    Block *block = nullptr;
    Cache::Handle *cache_handle = nullptr;

    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);
    // We intentionally allow extra stuff in index_value so that we
    // can add more features in the future.

    if (s.ok()) {
        handle.set_offset(handle.offset() + run->run_start_off);
        BlockContents contents;
//        if (block_cache != nullptr) {
//            char cache_key_buffer[16];
//            EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
//            EncodeFixed64(cache_key_buffer+8, handle.offset());
//            Slice key(cache_key_buffer, sizeof(cache_key_buffer));
//            cache_handle = block_cache->Lookup(key);
//            if (cache_handle != nullptr) {
//                block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
//            } else {
//                s = ReadBlock(table->rep_->file, options, handle, &contents);
//                if (s.ok()) {
//                    block = new Block(contents);
//                    if (contents.cachable && options.fill_cache) {
//                        cache_handle = block_cache->Insert(
//                                key, block, block->size(), &DeleteCachedBlock);
//                    }
//                }
//            }
//        } else {
//            s = ReadBlock(table->rep_->file, options, handle, &contents);
//            if (s.ok()) {
//                block = new Block(contents);
//            }
//        }
        s = ReadBlock(run->file, options, handle, &contents);
        if (s.ok()) {
            block = new Block(contents);
        }
    }

    Iterator *iter;
    if (block != nullptr) {
        iter = block->NewIterator(run->options->comparator);
        if (cache_handle == nullptr) {
            iter->RegisterCleanup(&DeleteBlock, block, nullptr);
        } else {
            iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
        }
    } else {
        iter = NewErrorIterator(s);
    }
    return iter;
}

Iterator *MiniRun::NewIterator(const ReadOptions &read_options) {
    return NewTwoLevelIterator(
            index_block.NewIterator(this->options->comparator),
            &MiniRun::BlockReader, const_cast<MiniRun *>(this), read_options);
}

}  // namespace silkstore
}  // namespace leveldb
