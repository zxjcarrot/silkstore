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

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

#include "leaf_store.h"
#include "segment.h"

namespace leveldb {
namespace silkstore {

class SilkStore : public DB {
 public:
    SilkStore(const Options &options, const std::string &dbname);

    virtual ~SilkStore();

    // Implementations of the DB interface
    virtual Status Put(const WriteOptions &, const Slice &key, const Slice &value);

    virtual Status Delete(const WriteOptions &, const Slice &key);

    virtual Status Write(const WriteOptions &options, WriteBatch *updates);

    virtual Status Get(const ReadOptions &options,
                       const Slice &key,
                       std::string *value);

    virtual Iterator *NewIterator(const ReadOptions &);

    virtual const Snapshot *GetSnapshot();

    virtual void ReleaseSnapshot(const Snapshot *snapshot);

    virtual bool GetProperty(const Slice &property, std::string *value);

    virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes);

    virtual void CompactRange(const Slice *begin, const Slice *end);

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
 private:

    friend class DB;

    struct CompactionState;
    struct Writer;

    // Constant after construction
    Env *const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_;  // options_.comparator == &internal_comparator_
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;


    // Leaf index
    DB *leaf_index_;

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock *db_lock_;

    // State below is protected by mutex_
    port::Mutex mutex_;
    port::AtomicPointer shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    MemTable *mem_;
    MemTable *imm_ GUARDED_BY(mutex_);  // Memtable being compacted

    port::AtomicPointer has_imm_;       // So bg thread can detect non-null imm_
    WritableFile *logfile_;
    uint64_t logfile_number_ GUARDED_BY(mutex_);
    log::Writer *log_;
    uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
    SequenceNumber max_sequence_ GUARDED_BY(mutex_);
    size_t memtable_capacity_ GUARDED_BY(mutex_);;
    SegmentManager* segment_manager_;
    // Queue of writers.
    std::deque<Writer *> writers_ GUARDED_BY(mutex_);
    WriteBatch *tmp_batch_ GUARDED_BY(mutex_);

    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

    // Has a background compaction been scheduled or is running?
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);

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

    CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

    // No copying allowed
    SilkStore(const SilkStore &);

    void operator=(const SilkStore &);

    const Comparator *user_comparator() const {
        return internal_comparator_.user_comparator();
    }

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    Status Recover() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status RecoverLogFile(uint64_t log_number, SequenceNumber *max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);;

    WriteBatch *BuildBatchGroup(Writer **last_writer)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeScheduleCompaction();

    void CompactionWork();

    static void BGWork(void* db);
    void BackgroundCall();

    LeafIndexEntry
    CompactLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s,
                std::string *buf, uint32_t start_minirun_no, uint32_t end_minirun_no);

    std::pair<uint32_t, uint32_t> ChooseLeafCompactionRunRange(const LeafIndexEntry &leaf_index_entry);

    Status
    SplitLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry,
              std::string *l1_max_key_buf, std::string *l2_max_key_buf, std::string *l1_index_entry_buf,
              std::string *l2_index_entry_buf);
    // silkstore stuff
    LeafStore * leaf_store_ = nullptr;
};

}  // namespace silkstore
}  // namespace leveldb

#endif  // SILKSTORE_DB_IMPL_H_
