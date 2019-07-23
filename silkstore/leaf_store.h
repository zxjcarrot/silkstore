//
// Created by zxjcarrot on 2019-07-15.
//

#ifndef SILKSTORE_LEAF_INDEX_H
#define SILKSTORE_LEAF_INDEX_H

#include <stdint.h>
#include <functional>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "table/block_builder.h"
#include "table/block.h"
#include "db/dbformat.h"

#include "segment.h"

namespace silkstore {
using namespace leveldb;

class MiniRunIndexEntry {
public:
    MiniRunIndexEntry(const Slice &data);

    Slice GetBlockIndexData() const;

    Slice GetFilterData() const;

    uint32_t GetSegmentNumber() const { return segment_number_; };

    uint32_t GetRunNumberWithinSegment() const { return run_no_within_segment_; };

    Slice GetRawData() const { return raw_data_; }


private:
    Slice raw_data_;
    uint32_t segment_number_;
    uint32_t run_no_within_segment_;
    uint32_t block_index_data_len_;
    uint32_t filter_data_len_;
};


class LeafIndexEntry {
public:
    LeafIndexEntry(const Slice &data = Slice());

    enum TraversalOrder {
        forward,
        backward
    };

    uint32_t GetNumMiniRuns() const;

    // Return all index entries of MiniRun sorted on insert time
    std::vector<MiniRunIndexEntry> GetAllMiniRunIndexEntry(TraversalOrder order = backward) const;

    // Iterate over all index entries of MiniRun with given order
    void ForEachMiniRunIndexEntry(std::function<bool(const MiniRunIndexEntry &, uint32_t)> processor,
                                  TraversalOrder order = backward) const;

    Slice GetRawData() const { return raw_data_; }

private:
    Slice raw_data_;
};

class LeafIndexEntryBuilder {
public:
    LeafIndexEntryBuilder() = delete;

    LeafIndexEntryBuilder(const LeafIndexEntryBuilder &) = delete;

    LeafIndexEntry operator=(const LeafIndexEntryBuilder &) = delete;

    static Status
    AppendMiniRunIndexEntry(const LeafIndexEntry &base,
                            const MiniRunIndexEntry &minirun_index_entry,
                            std::string *buf,
                            LeafIndexEntry *new_entry);

    static Status
    ReplaceMiniRunRange(const LeafIndexEntry &base, uint32_t start, uint32_t end, const MiniRunIndexEntry &replacement,
                        std::string *buf,
                        LeafIndexEntry *new_entry);
};


class LeafStore {
public:
    static Status
    Open(SegmentManager *seg_manager, leveldb::DB *leaf_index, const Options &options, const Comparator *user_cmp,
         LeafStore **store);

    Status Get(const ReadOptions &options, const LookupKey &key, std::string *value);

    Iterator* NewIterator(const ReadOptions &options);

private:
    LeafStore(SegmentManager *seg_manager, leveldb::DB *leaf_index, const Options &options,
              const Comparator *user_cmp) : seg_manager_(seg_manager), leaf_index_(leaf_index), options_(options),
                                            user_cmp_(user_cmp) {}

    SegmentManager *seg_manager_;
    leveldb::DB *leaf_index_;
    const Options options_;
    const Comparator *user_cmp_ = nullptr;
};
}
#endif //SILKSTORE_LEAF_INDEX_H
