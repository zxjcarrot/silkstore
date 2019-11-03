//
// Created by zxjcarrot on 2019-07-15.
//

#ifndef SILKSTORE_LEAF_INDEX_H
#define SILKSTORE_LEAF_INDEX_H

#include <climits>
#include <cstdint>
#include <functional>

#include "db/dbformat.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "table/block.h"
#include "db/db_iter.h"


namespace leveldb {
namespace silkstore {

class SegmentManager;
// format
//
class MiniRunIndexEntry {
 public:
    MiniRunIndexEntry(const Slice &data);

    Slice GetBlockIndexData() const;

    Slice GetFilterData() const;

    uint32_t GetSegmentNumber() const { return segment_number_; };

    uint32_t GetRunNumberWithinSegment() const { return run_no_within_segment_; };

    Slice GetRawData() const { return raw_data_; }

    size_t GetRunDataSize() const { return run_datasize_; }
    static MiniRunIndexEntry Build(uint32_t seg_no, uint32_t run_no, Slice block_index_data, Slice filter_data, size_t run_datasize, std::string * buf);

 private:
    Slice raw_data_;
    uint32_t segment_number_;
    uint32_t run_no_within_segment_;
    uint32_t block_index_data_len_;
    uint32_t filter_data_len_;
    uint32_t run_datasize_;
};

class LeafIndexEntry {
 public:
    LeafIndexEntry(const Slice &data = Slice());

    enum TraversalOrder {
        forward,
        backward
    };

    uint32_t GetNumMiniRuns() const;

    bool Empty() const { return GetNumMiniRuns() == 0; }

    // Return all index entries of MiniRun sorted on insert time
    std::vector<MiniRunIndexEntry> GetAllMiniRunIndexEntry(TraversalOrder order = backward) const;

    // Iterate over all index entries of MiniRun with given order
    void ForEachMiniRunIndexEntry(
        std::function<bool(const MiniRunIndexEntry &, uint32_t)> processor,
        TraversalOrder order = backward) const;

    Slice GetRawData() const { return raw_data_; }

    std::string ToString();
 private:
    Slice raw_data_;
};

class LeafIndexEntryBuilder {
 public:
    LeafIndexEntryBuilder() = delete;

    LeafIndexEntryBuilder(const LeafIndexEntryBuilder &) = delete;

    LeafIndexEntry operator=(const LeafIndexEntryBuilder &) = delete;

    static void AppendMiniRunIndexEntry(const LeafIndexEntry &base,
                                          const MiniRunIndexEntry &minirun_index_entry,
                                          std::string *buf,
                                          LeafIndexEntry *new_entry);

    static Status ReplaceMiniRunRange(const LeafIndexEntry &base,
                                      uint32_t start,
                                      uint32_t end,
                                      const MiniRunIndexEntry &replacement,
                                      std::string *buf,
                                      LeafIndexEntry *new_entry);

    static Status RemoveMiniRunRange(const LeafIndexEntry &base,
                                      uint32_t start,
                                      uint32_t end,
                                      std::string *buf,
                                      LeafIndexEntry *new_entry);
};

class LeafStore {
 public:
    static Status Open(SegmentManager *seg_manager, DB *leaf_index,
                       const Options &options, const Comparator *user_cmp,
                       LeafStore **store);

    Status Get(const ReadOptions &options, const LookupKey &key,
               std::string *value);

    Iterator* NewIterator(const ReadOptions &options);

    Iterator *NewIteratorForLeaf(const ReadOptions &options,
                                 const LeafIndexEntry &leaf_index_entry,
                                 Status &s,
                                 uint32_t start_minirun_no = 0,
                                 uint32_t end_minirun_no = std::numeric_limits<uint32_t>::max());

    Iterator *NewDBIterForLeaf(const ReadOptions &options, const LeafIndexEntry &leaf_index_entry, Status &s,
                               const Comparator *user_comparator,
                               SequenceNumber seq, uint32_t start_minirun_no = 0,
                               uint32_t end_minirun_no = std::numeric_limits<uint32_t>::max());

 private:
    class LeafStoreIterator;

    LeafStore(SegmentManager *seg_manager, DB *leaf_index,
              const Options &options, const Comparator *user_cmp)
        : seg_manager_(seg_manager),
          leaf_index_(leaf_index),
          options_(options),
          user_cmp_(user_cmp) {}

    SegmentManager *seg_manager_;
    DB *leaf_index_;
    const Options options_;
    const Comparator *user_cmp_ = nullptr;
};

}  // namespace silkstore
}  // namespace leveldb

#endif //SILKSTORE_LEAF_INDEX_H
