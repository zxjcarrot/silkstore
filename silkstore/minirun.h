//
// Created by zxjcarrot on 2019-06-29.
//

#ifndef SILKSTORE_MINIRUN_H
#define SILKSTORE_MINIRUN_H

#include <stdint.h>

#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"
#include "table/block.h"
#include "table/block_builder.h"

namespace leveldb {
namespace silkstore {

class SegmentBuilder;

/*
 * A minirun is a sorted run of key value pairs consisting of
 * one or more leveldb data blocks. It is the unit of update to a leaf.
 */
class MiniRun {
 public:
    Iterator *NewIterator(const ReadOptions &);

    MiniRun(const Options &options, RandomAccessFile *file,
            uint64_t off, uint64_t size, Block &index_block);
 private:
    static Iterator *BlockReader(void *, const ReadOptions &, const Slice &);
    const Options* options;
    RandomAccessFile *file;
    uint64_t run_start_off;
    uint64_t run_size;
    Block &index_block;
};

/*
 * A pointer that stores the location of minirun within a segment.
 */
typedef uint64_t MiniRunHandle;

class MiniRunBuilder {
 public:
    // Create a builder that will store the contents of the minirun it is
    // building in *file starting at file_offset.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    MiniRunBuilder(const Options &options, WritableFile *file, uint64_t file_offset);

    MiniRunBuilder(const MiniRunBuilder &) = delete;

    void operator=(const MiniRunBuilder &) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~MiniRunBuilder();

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    void Add(const Slice &key, const Slice &value);

    // Reset the builder as if it were just constructed.
    void Reset(uint64_t file_offset);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: Finish(), Abandon() have not been called
    void Flush();

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    Status Finish();

    // Return the index block for this minirun.
    // REQUIRES: Finish() has been called
    Slice IndexBlock() const;

    // Return the filter block for this minirun.
    // REQUIRES: Finish() has been called
    Slice FilterBlock() const;

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.  If invoked after a successful
    // Finish() call, returns the size of the final generated file.
    uint64_t FileSize() const;

 private:
    bool ok() const { return status().ok(); }

    void WriteBlock(BlockBuilder *block, BlockHandle *handle);

    void WriteRawBlock(const Slice &data, CompressionType, BlockHandle *handle);

    struct Rep;
    Rep *rep_;
};

}  // namespace silkstore
}  // namespace leveldb

#endif //SILKSTORE_MINIRUN_H
