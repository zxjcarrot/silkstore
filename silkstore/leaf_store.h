//
// Created by zxjcarrot on 2019-07-15.
//

#ifndef SILKSTORE_LEAF_INDEX_H
#define SILKSTORE_LEAF_INDEX_H

#include <climits>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "table/block.h"
#include "db/db_iter.h"
#include "util/mutexlock.h"
#include "leveldb/env.h"

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

    size_t GetLeafDataSize() const;
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

/*
 *
 * Stores the statistics of every leaf in memory:
 *  1. Write Hotness: measurement of update frequency of a leaf
 *  2. Group Id: which group this leaf belongs to. This is updated after
 *      each run of clustering algorithm on the leaves based on Write Hotness measure.
 *      Possible groups: Hot, Warm, Cold, Read-only.
 *  3. Read Hotness: measurement of read frequency of a leaf.
 *
 * The stats serve two purposes:
 *  1. To determine whether a leaf should be compacted to increase read performance.
 *  2. To group minruns with similar hotness together during merge to increase future GC efficiency.
 */
class LeafStatStore {
public:
    static constexpr int read_interval_in_micros = 5000000; // ten seconds
    static constexpr double read_hotness_exp_smooth_factor = 0.8;
    static constexpr double write_hotness_exp_smooth_factor = 0.8;

    struct LeafStat {
        int group_id;
        double write_hotness;
        // read_hotness = read_hotness_exp_smooth_factor * reads_in_last_interval + read_hotness * (1 - read_hotness_exp_smooth_factor)
        double read_hotness;
        long long last_write_time_in_s;
        long long reads_in_last_interval;
        int num_runs;
    };

    void NewLeaf(const std::string & key) {
        MutexLock g(&lock);
        m[key] = {-1, 0, 0, (long long)Env::Default()->NowMicros() / 1000000, 0, 0};
    }

    void IncrementLeafReads(const std::string & leaf_key) {
        MutexLock g(&lock);
        auto it = m.find(leaf_key);
        if (it == m.end())
            return;
        LeafStat & stat = it->second;
        ++stat.reads_in_last_interval;
    }

    double GetWriteHotness(const std::string & leaf_key) {
        MutexLock g(&lock);
        auto it = m.find(leaf_key);
        if (it == m.end())
            return -1;
        return it->second.write_hotness;
    }

    double GetReadHotness(const std::string & leaf_key) {
        MutexLock g(&lock);
        auto it = m.find(leaf_key);
        if (it == m.end())
            return -1;
        return it->second.read_hotness;
    }

    void DeleteLeaf(const std::string & leaf_key) {
        MutexLock g(&lock);
        m.erase(leaf_key);
    }

    void UpdateLeafNumRuns(const std::string & leaf_key, int num_runs) {
        MutexLock g(&lock);
        auto it = m.find(leaf_key);
        if (it == m.end()) {
            return;
        }
        LeafStat & stat = it->second;
        stat.num_runs = num_runs;
    }

    void UpdateWriteHotness(const std::string &leaf_key, int writes) {
        MutexLock g(&lock);
        auto it = m.find(leaf_key);
        if (it == m.end()) {
            g.Release();
            NewLeaf(leaf_key);
            UpdateWriteHotness(leaf_key, writes);
            return;
        }

        LeafStat & stat = it->second;
        long long cur_time_in_s = Env::Default()->NowMicros() / 1000000;
        // We weight the writes by the inverse of the amount of time elapsed since last update.
        // Therefore, the longer the elapsed time is, the less the writes contribute to the hotness.
        // This reflects not only the amount of writes but also the frequency of writes.
        double weighted_writes = (double)writes / std::max(1LL, cur_time_in_s - stat.last_write_time_in_s);
        stat.write_hotness = ExpSmoothUpdate(stat.write_hotness, weighted_writes, write_hotness_exp_smooth_factor);
        stat.last_write_time_in_s = cur_time_in_s;
    }

    void SplitLeaf(const std::string & leaf_key, const std::string & first_half_key) {
        // leaf_key is splitted into (first_half_key, leaf_key)
        MutexLock g(&lock);
        if (m.find(leaf_key) == m.end())
            return;
        LeafStat & first_half_leaf_stat = m[first_half_key] = {-1, 0, 0, (long long)Env::Default()->NowMicros() / 1000000, 0, 1};
        LeafStat & second_half_leaf_stat = m[leaf_key];
        first_half_leaf_stat.write_hotness = second_half_leaf_stat.write_hotness /= 2;
        first_half_leaf_stat.reads_in_last_interval = second_half_leaf_stat.reads_in_last_interval /= 2;
        first_half_leaf_stat.group_id = second_half_leaf_stat.group_id;
        first_half_leaf_stat.last_write_time_in_s = second_half_leaf_stat.last_write_time_in_s;
        second_half_leaf_stat.num_runs = 1;
    }


    void UpdateReadHotness() {
        MutexLock g(&lock);
        for (auto & kv : m) {
            UpdateReadHotnessForOneLeaf(kv.second);
        }
    }

    void ForEachLeaf(std::function<void(const std::string &, const LeafStat &)> processor) {
        MutexLock g(&lock);
        for (auto & kv : m) {
            processor(kv.first, kv.second);
        }
    }

private:

    double ExpSmoothUpdate(double old, double new_sample, double factor) {
        return old * (1 - factor) + new_sample * factor;
    }

    inline double ComputeReadHotness(double current_read_hotness, int current_interval_reads) {
        return current_read_hotness * (1 - read_hotness_exp_smooth_factor) + current_interval_reads * read_hotness_exp_smooth_factor;
    }

    void UpdateReadHotnessForOneLeaf(LeafStat & stat) {
        stat.read_hotness = ExpSmoothUpdate(stat.read_hotness, stat.reads_in_last_interval, read_hotness_exp_smooth_factor);
        stat.reads_in_last_interval = 0;
    }

    port::Mutex lock;
    std::unordered_map<std::string, LeafStat> m;
};

class LeafStore {
 public:
    static Status Open(SegmentManager *seg_manager, DB *leaf_index,
                       const Options &options, const Comparator *user_cmp,
                       LeafStore **store);

    Status Get(const ReadOptions &options, const LookupKey &key,
               std::string *value, LeafStatStore & stat_store);

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
