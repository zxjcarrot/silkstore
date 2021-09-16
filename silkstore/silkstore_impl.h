//
// Created by zxjcarrot on 2019-07-05.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef SILKSTORE_DB_IMPL_H_
#define SILKSTORE_DB_IMPL_H_

#include <deque>
#include <set>
//#include <list>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

#include "leaf_store.h"
#include "segment.h"


#include "nvm/nvmem.h"
#include "nvm/nvmlog.h"
#include "nvm/nvmmanager.h"
#include "db/nvmemtable.h"

namespace leveldb {
namespace silkstore {

class GroupedSegmentAppender;

class SilkStore : public DB {
public:
    SilkStore(const Options &options, const std::string &dbname);

    virtual ~SilkStore();

    // Implementations of the DB interface
    virtual Status Put(const WriteOptions &, const Slice &key, const Slice &value);

    virtual Status Delete(const WriteOptions &, const Slice &key);

    virtual Status Write(const WriteOptions &options, WriteBatch *updates);
    virtual Status NvmWrite(const WriteOptions &options, NvmWriteBatch *updates);

    virtual Status Get(const ReadOptions &options,
                       const Slice &key,
                       std::string *value);

    virtual Iterator *NewIterator(const ReadOptions &);

    virtual const Snapshot *GetSnapshot();

    virtual void ReleaseSnapshot(const Snapshot *snapshot);

    virtual bool GetProperty(const Slice &property, std::string *value);

    virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {}

    virtual void CompactRange(const Slice *begin, const Slice *end) {}

    // Extra methods (for testing) that are not in the public DB interface

    // Compact any files in the named level that overlap [*begin,*end]
    void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

    // Force current memtable contents to be compacted.
    Status TEST_CompactMemTable();

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h).
    // The returned iterator should be deleted when no longer needed.
    Iterator *TEST_NewInternalIterator();

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t TEST_MaxNextLevelOverlappingBytes();

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.
    void RecordReadSample(Slice key);


    Status OpenIndex(const Options &index_options);

    void BackgroundCompaction();

    Status CopyMinirunRun(Slice leaf_max_key, LeafIndexEntry &index_entry, uint32_t run_idx_in_index_entry,
                          SegmentBuilder *seg_builder, WriteBatch & leaf_index_wb);

    Status GarbageCollectSegment(Segment *seg, GroupedSegmentAppender &appender, WriteBatch & leaf_index_wb);

    int GarbageCollect();

    std::string SegmentsSpaceUtilityHistogram();

    void Destroy();

    void MergeImm(NvmemTable* newImm);

private:

    friend class DB;

    struct CompactionState;
    struct Writer;
    struct NvmWriter;


    // Constant after construction
    Env *const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_;  // options_.comparator == &internal_comparator_
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;

    Options leaf_index_options_;  // options_.comparator == &internal_comparator_

    // Leaf index
    DB *leaf_index_;

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock *db_lock_;

    port::Mutex GCMutex;

    // State below is protected by mutex_
    port::Mutex mutex_;
    port::AtomicPointer shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    NvmemTable *mem_;
    //std::list<std::pair<int, NvmemTable*>> imm_ GUARDED_BY(mutex_);  // Memtable being compacted
    
    std::deque<NvmemTable*> imm_ GUARDED_BY(mutex_);  // Memtable being compacted


//    NvmemTable *imm_ GUARDED_BY(mutex_);  // Memtable being compacted

    port::AtomicPointer has_imm_;       // So bg thread can detect non-null imm_
    WritableFile *logfile_;
    uint64_t logfile_number_ GUARDED_BY(mutex_);
    log::Writer *log_;
    uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
    SequenceNumber max_sequence_ GUARDED_BY(mutex_);
    size_t memtable_capacity_ GUARDED_BY(mutex_);;
    size_t allowed_num_leaves = 0;
    size_t num_leaves = 0;
    silkstore::NvmManager *nvm_manager_;
    silkstore::NvmLog *nvm_log_; 
    SegmentManager *segment_manager_;
    // Queue of writers.
    std::deque<Writer *> writers_ GUARDED_BY(mutex_);
    std::deque<NvmWriter *> nvmwriters_ GUARDED_BY(mutex_);
    WriteBatch *tmp_batch_ GUARDED_BY(mutex_);
    NvmWriteBatch *nvm_tmp_batch_ GUARDED_BY(mutex_);


    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

    // Has a background compaction been scheduled or is running?
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);

    std::function<void()> leaf_optimization_func_;
    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey *begin;   // null means beginning of key range
        const InternalKey *end;     // null means end of key range
        InternalKey tmp_storage;    // Used to keep track of compaction progress
    };
    ManualCompaction *manual_compaction_ GUARDED_BY(mutex_);

    // Have we encountered a background error in paranoid mode?
    Status bg_error_ GUARDED_BY(mutex_);

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;

        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats &c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }
    };


    // No copying allowed
    SilkStore(const SilkStore &);

    void operator=(const SilkStore &);

    const Comparator *user_comparator() const {
        return internal_comparator_.user_comparator();
    }

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status NvmMakeRoomForWrite(bool force /* compact even if there is room? */)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    Status Recover() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status RecoverLogFile(uint64_t log_number, SequenceNumber *max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);;

    WriteBatch *BuildBatchGroup(Writer **last_writer)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    NvmWriteBatch *NvmBuildBatchGroup(NvmWriter **last_writer)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeScheduleCompaction();

    Status DoCompactionWork(WriteBatch &leaf_index_wb, NvmemTable*  imm);

    Status OptimizeLeaf();

    Status MakeRoomInLeafLayer(bool force = false);

    static void BGWork(void *db);

    void BackgroundCall();

    Status InvalidateLeafRuns(const LeafIndexEntry &leaf_index_entry, size_t start_run, size_t end_run);

    LeafIndexEntry
    CompactLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s,
                std::string *buf, uint32_t start_minirun_no, uint32_t end_minirun_no,
                const Snapshot *leaf_index_snap = nullptr);

    std::pair<uint32_t, uint32_t> ChooseLeafCompactionRunRange(const LeafIndexEntry &leaf_index_entry);


    // silkstore stuff
    LeafStore *leaf_store_ = nullptr;
    LeafStatStore stat_store_;

    struct MergeStats {
        size_t bytes_written = 0;
        size_t bytes_read = 0;

        size_t gc_bytes_written = 0;
        size_t gc_bytes_read = 0;
        size_t gc_bytes_read_unopt = 0;

        // # miniruns queried in leaf_index_ for validness during GC.
        size_t gc_miniruns_queried;
        // # miniruns in total checked during GC.
        // gc_miniruns_total - gc_miniruns_queried => # miniruns that are skipped by
        size_t gc_miniruns_total;

        void Add(size_t read, size_t written) {
            bytes_read += read;
            bytes_written += written;
        }

        void AddGCUnoptStats(size_t read) {
            gc_bytes_read_unopt += read;
        }

        void AddGCStats(size_t read, size_t written) {
            gc_bytes_written += written;
            gc_bytes_read += read;
        }

        void AddGCMiniRunStats(size_t miniruns_queried, size_t miniruns_total) {
            gc_miniruns_queried += miniruns_queried;
            gc_miniruns_total += miniruns_total;
        }

        size_t time_spent_compaction = 0;
        size_t time_spent_gc = 0;

        void AddTimeCompaction(size_t t) {
            time_spent_compaction += t;
        }

        void AddTimeGC(size_t t) {
            time_spent_gc += t;
        }
    } stats_;
};

Status DestroyDB(const std::string &dbname, const Options &options);

}  // namespace silkstore
}  // namespace leveldb

#endif  // SILKSTORE_DB_IMPL_H_
