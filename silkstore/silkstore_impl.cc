//
// Created by zxjcarrot on 2019-07-05.
//

#include <thread>
#include <queue>
#include <memory>
#include <functional>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/write_batch.h"
#include "table/merger.h"
#include "util/mutexlock.h"

#include "silkstore/silkstore_impl.h"
#include "silkstore/silkstore_iter.h"
#include "silkstore/util.h"

int runs_searched = 0;
namespace leveldb {

Status DB::OpenSilkStore(const Options &options,
                         const std::string &name,
                         DB **dbptr) {
    Options silkstore_options = options;
    silkstore_options.env = Env::NewPosixEnv();
    *dbptr = nullptr;
    silkstore::SilkStore *store = new silkstore::SilkStore(silkstore_options, name);
    Status s = store->Recover();
    if (s.ok()) {
        *dbptr = store;
        return s;
    } else {
        delete store;
        return s;
    }
}

namespace silkstore {

const std::string kCURRENTFilename = "CURRENT";

// Fix user-supplied options to be reasonable
template<class T, class V>
static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

static Options SanitizeOptions(const std::string &dbname,
                               const InternalKeyComparator *icmp,
                               const InternalFilterPolicy *ipolicy,
                               const Options &src) {
    Options result = src;
    result.comparator = icmp;
    result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
    ClipToRange(&result.max_open_files, 64 + 10, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&result.block_size, 1 << 10, 4 << 20);
    if (result.info_log == nullptr) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname);  // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            result.info_log = nullptr;
        }
    }
    return result;
}

SilkStore::SilkStore(const Options &raw_options, const std::string &dbname)
        : env_(raw_options.env),
          internal_comparator_(raw_options.comparator),
          internal_filter_policy_(raw_options.filter_policy),
          options_(SanitizeOptions(dbname, &internal_comparator_,
                                   &internal_filter_policy_, raw_options)),
          owns_info_log_(options_.info_log != raw_options.info_log),
          owns_cache_(options_.block_cache != raw_options.block_cache),
          dbname_(dbname),
          leaf_index_(nullptr),
          db_lock_(nullptr),
          shutting_down_(nullptr),
          background_work_finished_signal_(&mutex_),
          mem_(nullptr),
          imm_(nullptr),
          logfile_(nullptr),
          logfile_number_(0),
          log_(nullptr),
          max_sequence_(0),
          memtable_capacity_(options_.write_buffer_size),
          seed_(0),
          tmp_batch_(new WriteBatch),
          background_compaction_scheduled_(false),
          leaf_optimization_func_([](){}),
          manual_compaction_(nullptr) {
    has_imm_.Release_Store(nullptr);
}


SilkStore::~SilkStore() {
    // Wait for background work to finish
    mutex_.Lock();
    shutting_down_.Release_Store(this);  // Any non-null value is ok
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();

    // Delete leaf index
    delete leaf_index_;
    leaf_index_ = nullptr;

    if (db_lock_ != nullptr) {
        env_->UnlockFile(db_lock_);
    }

//    delete versions_;
    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
//    delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    if (owns_cache_) {
        delete options_.block_cache;
    }
}

Status SilkStore::OpenIndex(const Options &index_options) {
    assert(leaf_index_ == nullptr);
    Status s = DB::Open(index_options, dbname_ + "/leaf_index", &leaf_index_);
    return s;
}


static std::string MakeFileName(const std::string &dbname, uint64_t number,
                                const char *prefix, const char *suffix) {
    char buf[100];
    snprintf(buf, sizeof(buf), "/%s%06llu.%s", prefix,
             static_cast<unsigned long long>(number),
             suffix);
    return dbname + buf;
}

static std::string LogFileName(const std::string &dbname, uint64_t number) {
    assert(number > 0);
    return MakeFileName(dbname, number, "", "log");
}

static std::string CurrentFilename(const std::string &dbname) {
    return dbname + "/" + kCURRENTFilename;
}

Status SilkStore::RecoverLogFile(uint64_t log_number, SequenceNumber *max_sequence) {
    struct LogReporter : public log::Reader::Reporter {
        Env *env;
        Logger *info_log;
        const char *fname;
        Status *status;  // null if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status &s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == nullptr ? "(ignoring error) " : ""),
                fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != nullptr && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_, log_number);
    SequentialFile *file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true/*checksum*/,
                       0/*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long) log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable *mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) &&
           status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(
                    record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }
        status = WriteBatchInternal::InsertInto(&batch, mem);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq =
                WriteBatchInternal::Sequence(&batch) +
                WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }

    }

    delete file;


    // reuse the last log file
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
        Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
        log_ = new log::Writer(logfile_, lfile_size);
        logfile_number_ = log_number;
        if (mem != nullptr) {
            mem_ = mem;
            mem = nullptr;
        } else {
            // mem can be nullptr if lognum exists but was empty.
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
        }
    }

    if (mem != nullptr) {
        // mem did not get reused; delete it.
        mem->Unref();
    }

    return status;
}

Status SilkStore::Recover() {
    MutexLock g(&mutex_);
    this->leaf_index_options_.create_if_missing = true;
    this->leaf_index_options_.filter_policy = NewBloomFilterPolicy(10);
    this->leaf_index_options_.block_cache = NewLRUCache(8 << 26);
    this->leaf_index_options_.compression = kNoCompression;
    Status s = OpenIndex(this->leaf_index_options_);

    if (!s.ok()) return s;

    // Open segment manager
    s = SegmentManager::OpenManager(this->options_, dbname_, &segment_manager_, std::bind(&SilkStore::GarbageCollect, this));
    if (!s.ok()) return s;

    s = LeafStore::Open(segment_manager_, leaf_index_, options_, internal_comparator_.user_comparator(), &leaf_store_);
    if (!s.ok()) return s;

    std::string current_content;
    s = ReadFileToString(env_, CurrentFilename(dbname_), &current_content);
    if (s.IsNotFound()) {
        // new db
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
        SequenceNumber log_start_seq_num = max_sequence_ = 1;
        WritableFile *lfile = nullptr;
        s = env_->NewWritableFile(LogFileName(dbname_, log_start_seq_num), &lfile);
        if (!s.ok()) return s;

        logfile_ = lfile;
        log_ = new log::Writer(logfile_);
        std::string temp_current = dbname_ + "/" + "CURRENT_temp";
        s = WriteStringToFile(env_, std::to_string(log_start_seq_num), temp_current);
        if (!s.ok()) return s;
        s = env_->RenameFile(temp_current, CurrentFilename(dbname_));
    } else {
        Iterator *it = leaf_index_->NewIterator(ReadOptions{});
        DeferCode c([it]() { delete it; });
        it->SeekToFirst();
        num_leaves = 0;
        while (it->Valid()) {
            ++num_leaves;
            it->Next();
        }
        allowed_num_leaves = num_leaves;
        SequenceNumber log_start_seq_num = std::stoi(current_content);
        s = RecoverLogFile(log_start_seq_num, &max_sequence_);
    }
    if (!s.ok())
        return s;

    leaf_optimization_func_ = [this]() {
        this->OptimizeLeaf();
        //this->GarbageCollect();
        env_->ScheduleDelayedTask(leaf_optimization_func_, LeafStatStore::read_interval_in_micros);
    };
    env_->ScheduleDelayedTask(leaf_optimization_func_, LeafStatStore::read_interval_in_micros);
    return s;
}

Status SilkStore::TEST_CompactMemTable() {
    // nullptr batch means just wait for earlier writes to be done
    Status s = Write(WriteOptions(), nullptr);
    if (s.ok()) {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (imm_ != nullptr && bg_error_.ok()) {
            background_work_finished_signal_.Wait();
        }
        if (imm_ != nullptr) {
            s = bg_error_;
        }
    }
    return s;
}

// Convenience methods
Status SilkStore::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
    //fprintf(stderr, "put key: %s, seqnum: %u\n", key.ToString().c_str(), max_sequence_);
    return DB::Put(o, key, val);
}

Status SilkStore::Delete(const WriteOptions &options, const Slice &key) {
    return DB::Delete(options, key);
}

static void SilkStoreNewIteratorCleanup(void *arg1, void *arg2) {
    static_cast<MemTable *>(arg1)->Unref();
    if (arg2) static_cast<MemTable *>(arg2)->Unref();
}

const Snapshot * SilkStore::GetSnapshot() {
    MutexLock l(&mutex_);
    return leaf_index_->GetSnapshot();
}

void SilkStore::ReleaseSnapshot(const Snapshot *snapshot) {
    MutexLock l(&mutex_);
    return leaf_index_->ReleaseSnapshot(snapshot);
}

Iterator *SilkStore::NewIterator(const ReadOptions &ropts) {
    MutexLock l(&mutex_);
    SequenceNumber seqno = ropts.snapshot ? dynamic_cast<const SnapshotImpl *>(ropts.snapshot)->sequence_number() : max_sequence_;
    // Collect together all needed child iterators
    std::vector < Iterator * > list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
        imm_->Ref();
    }
    list.push_back(leaf_store_->NewIterator(ropts));
    Iterator *internal_iter =
            NewMergingIterator(&internal_comparator_, &list[0], list.size());
    internal_iter->RegisterCleanup(SilkStoreNewIteratorCleanup, mem_, imm_);
    return leveldb::silkstore::NewDBIterator(internal_comparator_.user_comparator(), internal_iter,
                                             seqno);
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status SilkStore::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    bool allow_delay = !force;
    Status s;
    while (true) {
        size_t memtbl_size = mem_->ApproximateMemoryUsage();
        if (!force && (memtbl_size <= memtable_capacity_)) {
            break;
        } else if (imm_ != nullptr) {
            Log(options_.info_log, "Current memtable full;Compaction ongoing; waiting...\n");
            background_work_finished_signal_.Wait();
        } else {
            // Attempt to switch to a new memtable and trigger compaction of old
            uint64_t new_log_number = max_sequence_;
            WritableFile *lfile = nullptr;
            s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
            if (!s.ok()) {
                break;
            }
            delete log_;
            delete logfile_;
            logfile_ = lfile;
            logfile_number_ = new_log_number;
            log_ = new log::Writer(lfile);
            imm_ = mem_;
            has_imm_.Release_Store(imm_);
            size_t old_memtable_capacity = memtable_capacity_;
            size_t new_memtable_capacity =
                    (memtable_capacity_ + segment_manager_->ApproximateSize()) / options_.memtbl_to_L0_ratio;
            new_memtable_capacity = std::min(options_.max_memtbl_capacity,
                                             std::max(options_.write_buffer_size, new_memtable_capacity));
            Log(options_.info_log, "new memtable capacity %llu\n", new_memtable_capacity);
            memtable_capacity_ = new_memtable_capacity;
            allowed_num_leaves = std::ceil(new_memtable_capacity / (options_.storage_block_size + 0.0));
            DynamicFilter * dynamic_filter = nullptr;
            if (options_.use_memtable_dynamic_filter) {
                size_t imm_num_entries = imm_->NumEntries();
                size_t new_memtable_capacity_num_entries = imm_num_entries * std::ceil(new_memtable_capacity / (old_memtable_capacity + 0.0));
                assert(new_memtable_capacity_num_entries);
                dynamic_filter = NewDynamicFilterBloom(new_memtable_capacity_num_entries, options_.memtable_dynamic_filter_fp_rate);
            }
            mem_ = new MemTable(internal_comparator_, dynamic_filter);
            mem_->Ref();
            force = false;   // Do not force another compaction if have room
            MaybeScheduleCompaction();
        }
    }

    return s;
}


void SilkStore::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);
    if (shutting_down_.Acquire_Load()) {
        // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
    } else {
        BackgroundCompaction();
    }

    background_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    MaybeScheduleCompaction();
    background_work_finished_signal_.SignalAll();
}

void SilkStore::BGWork(void *db) {
    reinterpret_cast<SilkStore *>(db)->BackgroundCall();
}

void SilkStore::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    if (background_compaction_scheduled_) {
        // Already scheduled
    } else if (shutting_down_.Acquire_Load()) {
        // DB is being deleted; no more background compactions
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
    } else if (imm_ == nullptr &&
               manual_compaction_ == nullptr) {
        // No work to be done
    } else {
        background_compaction_scheduled_ = true;
        env_->Schedule(&SilkStore::BGWork, this);
    }
}


// Information kept for every waiting writer
struct SilkStore::Writer {
    Status status;
    WriteBatch *batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex *mu) : cv(mu) {}
};


Status SilkStore::Write(const WriteOptions &options, WriteBatch *my_batch) {
    Writer w(&mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    writers_.push_back(&w);
    while (!w.done && &w != writers_.front()) {
        w.cv.Wait();
    }
    if (w.done) {
        return w.status;
    }

    // May temporarily unlock and wait.
    Status status = MakeRoomForWrite(my_batch == nullptr);
    uint64_t last_sequence = max_sequence_;
    Writer *last_writer = &w;
    if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
        WriteBatch *updates = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(updates, last_sequence + 1);
        last_sequence += WriteBatchInternal::Count(updates);

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        {
            mutex_.Unlock();
            status = log_->AddRecord(WriteBatchInternal::Contents(updates));
            bool sync_error = false;
            if (status.ok() && options.sync) {
                status = logfile_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(updates, mem_);
            }
            mutex_.Lock();
            if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                //RecordBackgroundError(status);
                bg_error_ = status;
            }
        }
        if (updates == tmp_batch_) tmp_batch_->Clear();

        max_sequence_ = last_sequence;
    }

    while (true) {
        Writer *ready = writers_.front();
        writers_.pop_front();
        if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    // Notify new head of write queue
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return status;
}

bool SilkStore::GetProperty(const Slice &property, std::string *value) {
    if (property.ToString() == "silkstore.runs_searched") {
        *value = std::to_string(runs_searched);
        return true;
    } else if (property.ToString() == "silkstore.num_leaves") {
        auto it = leaf_index_->NewIterator(ReadOptions{});
        DeferCode c([it](){delete it;});
        int cnt = 0;
        it->SeekToFirst();
        while (it->Valid()) {
            ++cnt;
            it->Next();
        }
        *value = std::to_string(cnt);
        return true;
    } else if (property.ToString() == "silkstore.leaf_stats") {
        auto it = leaf_index_->NewIterator(ReadOptions{});
        DeferCode c([it](){delete it;});
        int cnt = 0;
        it->SeekToFirst();
        while (it->Valid()) {
            ++cnt;
            auto key = it->key();
            LeafIndexEntry index_entry(it->value());
            value->append(key.ToString());
            value->append("->");
            value->append(index_entry.ToString());
            value->append(" ");
            it->Next();
        }
        return true;
    } else if (property.ToString() == "silkstore.leaf_avg_num_runs") {
        auto it = leaf_index_->NewIterator(ReadOptions{});
        DeferCode c([it](){delete it;});
        int leaf_cnt = 0;
        int run_cnt = 0;
        it->SeekToFirst();
        while (it->Valid()) {
            ++leaf_cnt;
            auto key = it->key();
            LeafIndexEntry index_entry(it->value());
            run_cnt += index_entry.GetNumMiniRuns();
            it->Next();
        }
        *value = std::to_string(run_cnt / (leaf_cnt + 0.001));
        return true;
    } else if (property.ToString() == "silkstore.searches_in_memtable") {
        MutexLock g(&mutex_);
        size_t res = mem_->Searches();
        if (imm_) {
            res += imm_->Searches();
        }
        *value = std::to_string(res);
    }
    return false;
}

Status SilkStore::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value) {

    Status s;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;
    if (options.snapshot != nullptr) {
        snapshot =
                static_cast<const SnapshotImpl *>(options.snapshot)->sequence_number();
    } else {
        snapshot = max_sequence_;
    }
    //fprintf(stderr, "Get key: %s, seqnum: %u\n", key.ToString().c_str(), snapshot);
    MemTable *mem = mem_;
    MemTable *imm = imm_;
    mem->Ref();
    if (imm != nullptr) imm->Ref();


    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        // First look in the memtable, then in the immutable memtable (if any).
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s)) {
            // Done
        } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
            // Done
        } else {
            s = leaf_store_->Get(options, lkey, value, stat_store_);
        }
        mutex_.Lock();
    }

//    if (have_stat_update && current->UpdateStats(stats)) {
//        MaybeScheduleCompaction();
//    }
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    return s;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch *SilkStore::BuildBatchGroup(Writer **last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer *first = writers_.front();
    WriteBatch *result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    size_t max_size = 1 << 20;
    if (size <= (128 << 10)) {
        max_size = size + (128 << 10);
    }

    *last_writer = first;
    std::deque<Writer *>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer *w = *iter;
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }

        if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}

class GroupedSegmentAppender {
public:
    GroupedSegmentAppender(int num_groups, SegmentManager * segment_manager, const Options & options, bool gc_on_segment_shortage = true) : builders(num_groups), segment_manager(segment_manager), options(options), gc_on_segment_shortage(gc_on_segment_shortage) {}

    // Make sure the segment that is being built by a group has enough space.
    // If not, finish off the old segment and create a new segment.
    // Return the builder of the designated group.
    Status MakeRoomForGroupAndGetBuilder(uint32_t group_id, SegmentBuilder **builder_ptr) {
        assert(group_id >= 0 && group_id < builders.size());
        if (builders[group_id] != nullptr && builders[group_id]->FileSize() < options.segment_file_size_thresh) {
            // Segment of the group is in good shape, return its builder directly.
            *builder_ptr = builders[group_id];
            return Status::OK();
        } else if (builders[group_id] != nullptr &&
                   builders[group_id]->FileSize() >= options.segment_file_size_thresh) {
            // Segment filled up
            Status s = builders[group_id]->Finish();
            if (!s.ok()) {
                return s;
            }
            delete builders[group_id];
            builders[group_id] = nullptr;
        }
        uint32_t seg_id;
        std::unique_ptr<SegmentBuilder> new_builder;
        Status s = segment_manager->NewSegmentBuilder(&seg_id, new_builder, gc_on_segment_shortage);
        if (!s.ok()) {
            return s;
        }
        *builder_ptr = builders[group_id] = new_builder.release();
        return Status::OK();
    }

    ~GroupedSegmentAppender() {
        // Finish off unfinished segments
        for (size_t i = 0; i < builders.size(); ++i) {
            if (builders[i] == nullptr)
                continue;
            builders[i]->Finish();
            delete builders[i];
            builders[i] = nullptr;
        }
    }

private:
    std::vector<SegmentBuilder*> builders;
    SegmentManager * segment_manager;
    Options options;
    bool gc_on_segment_shortage;
};

std::pair <uint32_t, uint32_t> SilkStore::ChooseLeafCompactionRunRange(const LeafIndexEntry &leaf_index_entry) {
    // TODO: come up with a better approach
    uint32_t num_runs = leaf_index_entry.GetNumMiniRuns();
    assert(num_runs > 1);
    return {num_runs - 2, num_runs - 1};
}

Status
SilkStore::SplitLeaf(SegmentBuilder *seg_builder, uint32_t seg_id, const LeafIndexEntry &leaf_index_entry,
                     SequenceNumber seq_num,
                     std::string *l1_max_key_buf, std::string *l2_max_key_buf,
                     std::string *l1_index_entry_buf, std::string *l2_index_entry_buf) {
    // TODO: implement leaf split
    Status s;
    /* We use DBIter to get the most recent non-deleted keys. */
    auto it = dynamic_cast<silkstore::DBIter *>(leaf_store_->NewDBIterForLeaf(ReadOptions{}, leaf_index_entry, s,
                                                                              user_comparator(), seq_num));

    DeferCode c([it]() { delete it; });

    it->SeekToFirst();
    size_t bytes_total = 0;
    size_t key_count = 0;
    size_t first_key_bytes = 0;
    while (it->Valid()) {
        //keys.push_back(it->key().ToString());
        bytes_total += it->internal_key().size() + it->value().size();
        ++key_count;
        if (key_count == 1) {
            first_key_bytes = bytes_total;
        }

        it->Next();
    }
    assert(key_count >= 1);
    if (key_count == 1 || bytes_total < options_.leaf_datasize_thresh / 2) {
        return Status::SplitUnderflow("Insufficient key count after split");
    }
    assert(key_count >= 2);
    size_t pivot_point = std::max(first_key_bytes, bytes_total / 2);

    it->SeekToFirst();
    seg_builder->StartMiniRun();
    size_t bytes = 0;

    // first half
    while (it->Valid()) {
        bytes += it->internal_key().size() + it->value().size();
        /*
         * Since splitting a leaf should preserve the sequence numbers of the most recent non-deleted keys,
         * we modified DBIter to provide access to its internal key representation.
         * */
        seg_builder->Add(it->internal_key(), it->value());
        --key_count;
        if (key_count == 1 || bytes >= pivot_point) {
            uint32_t run_no;
            seg_builder->FinishMiniRun(&run_no);
            l1_max_key_buf->assign(it->key().data(), it->key().size());
            std::string buf;
            MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                     seg_builder->GetFinishedRunIndexBlock(),
                                     seg_builder->GetFinishedRunFilterBlock(),
                                     seg_builder->GetFinishedRunDataSize(),
                                     &buf);
            LeafIndexEntry new_leaf_index_entry;
            LeafIndexEntryBuilder::AppendMiniRunIndexEntry(LeafIndexEntry{}, minirun_index_entry, l1_index_entry_buf,
                                                           &new_leaf_index_entry);
            it->Next();
            break;
        }
        it->Next();
    }

    // second half
    assert(it->Valid());
    seg_builder->StartMiniRun();
    while (it->Valid()) {
        // Same reasoning as above
        seg_builder->Add(it->internal_key(), it->value());
        l2_max_key_buf->assign(it->key().data(), it->key().size());
        it->Next();
    }
    {
        uint32_t run_no;
        seg_builder->FinishMiniRun(&run_no);
        std::string buf;
        MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                 seg_builder->GetFinishedRunIndexBlock(),
                                 seg_builder->GetFinishedRunFilterBlock(),
                                 seg_builder->GetFinishedRunDataSize(),
                                 &buf);
        LeafIndexEntry new_leaf_index_entry;
        LeafIndexEntryBuilder::AppendMiniRunIndexEntry(LeafIndexEntry{}, minirun_index_entry, l2_index_entry_buf,
                                                       &new_leaf_index_entry);
        assert(it->Valid() == false);
    }
    return s;
}

LeafIndexEntry
SilkStore::CompactLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s,
                       std::string *buf, uint32_t start_minirun_no, uint32_t end_minirun_no) {
    buf->clear();
    bool cover_whole_range = end_minirun_no - start_minirun_no + 1 == leaf_index_entry.GetNumMiniRuns();
    Iterator *it = leaf_store_->NewIteratorForLeaf(ReadOptions{}, leaf_index_entry, s, start_minirun_no,
                                                   end_minirun_no);
    if (!s.ok()) return {};
    DeferCode c([it]() { delete it; });

    it->SeekToFirst();
    std::string current_user_key;
    bool has_current_user_key = false;
    size_t num_unique_keys = 0, keys = 0;
    while (it->Valid()) {
        Slice key = it->key();
        ++keys;
        ParsedInternalKey ikey;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
        } else {

            auto itvalue = it->value();
            if (!has_current_user_key ||
                user_comparator()->Compare(ikey.user_key,
                                           Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;

                if (cover_whole_range && ikey.type == kTypeDeletion) {
                    // If all miniruns are compacted into one and the key type is Deletion,
                    // then we can deleting this key physically by not adding it to the final compacted run.
                } else {
                    if (seg_builder->RunStarted() == false) {
                        s = seg_builder->StartMiniRun();
                        if (!s.ok())return {};
                    }
                    ++num_unique_keys;
                    seg_builder->Add(it->key(), itvalue);
                }
            }
        }
        it->Next();
    }

    LeafIndexEntry new_leaf_index_entry;

    if (seg_builder->RunStarted() == false) {
        // The result of the compacted range is empty, remove them from the index entry
        s = LeafIndexEntryBuilder::RemoveMiniRunRange(leaf_index_entry, start_minirun_no, end_minirun_no,
                                                      buf, &new_leaf_index_entry);
    } else {
        uint32_t run_no;
        seg_builder->FinishMiniRun(&run_no);
        // Otherwise, replace the compacted range minirun index entries with the result minirun index entry
        std::string buf2;
        MiniRunIndexEntry replacement = MiniRunIndexEntry::Build(seg_no, run_no,
                                                                 seg_builder->GetFinishedRunIndexBlock(),
                                                                 seg_builder->GetFinishedRunFilterBlock(),
                                                                 seg_builder->GetFinishedRunDataSize(),
                                                                 &buf2);
        s = LeafIndexEntryBuilder::ReplaceMiniRunRange(leaf_index_entry, start_minirun_no, end_minirun_no, replacement,
                                                       buf, &new_leaf_index_entry);
    }

    if (!s.ok()) return {};
    return new_leaf_index_entry;
}

Status SilkStore::CopyMinirunRun(Slice leaf_max_key, LeafIndexEntry &leaf_index_entry, uint32_t run_idx_in_index_entry,
                                 SegmentBuilder *target_seg_builder) {
    Status s;
    assert(run_idx_in_index_entry < leaf_index_entry.GetNumMiniRuns());
    std::unique_ptr<Iterator> source_it(
            leaf_store_->NewIteratorForLeaf({}, leaf_index_entry, s, run_idx_in_index_entry, run_idx_in_index_entry));
    if (!s.ok())
        return s;
    assert(target_seg_builder->RunStarted() == false);
    source_it->SeekToFirst();
    s = target_seg_builder->StartMiniRun();
    if (!s.ok())
        return s;
    while (source_it->Valid()) {
        target_seg_builder->Add(source_it->key(), source_it->value());
        source_it->Next();
    }
    uint32_t run_no;
    s = target_seg_builder->FinishMiniRun(&run_no);
    if (!s.ok())
        return s;
    std::string buf;
    buf.clear();
    MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(target_seg_builder->SegmentId(), run_no,
                                                                         target_seg_builder->GetFinishedRunIndexBlock(),
                                                                         target_seg_builder->GetFinishedRunFilterBlock(),
                                                                         target_seg_builder->GetFinishedRunDataSize(),
                                                                         &buf);
    LeafIndexEntry new_leaf_index_entry;
    std::string buf2;
    s = LeafIndexEntryBuilder::ReplaceMiniRunRange(leaf_index_entry, run_idx_in_index_entry, run_idx_in_index_entry,
                                                   new_minirun_index_entry,
                                                   &buf2, &new_leaf_index_entry);
    if (!s.ok())
        return s;
    return leaf_index_->Put({}, leaf_max_key, new_leaf_index_entry.GetRawData());
    //leaf_index_wb.insert(std::make_pair(.ToString(), new_leaf_index_entry.GetRawData().ToString()));
    //return s;
}

Status SilkStore::GarbageCollectSegment(Segment *seg, GroupedSegmentAppender &appender) {
    Status s;
    size_t copied = 0;
    size_t segment_size = seg->SegmentSize();

    seg->ForEachRun([&, this](int run_no, MiniRunHandle run_handle, size_t run_size, bool valid) {
        if (valid == false) // Skip invalidated runs
            return false;
        // We take out the first key of the last block in the run to query leaf_index for validness
        MiniRun *run;
        Block index_block(BlockContents({Slice(), false, false}));
        s = seg->OpenMiniRun(run_no, index_block, &run);
        if (!s.ok()) // error, early exit
            return true;
        DeferCode c([run]() { delete run; });
        BlockHandle last_block_handle = run_handle.last_block_handle;

        std::unique_ptr<Iterator> block_it(
                run->NewIteratorForOneBlock({}, last_block_handle));

        block_it->SeekToFirst();
        if (block_it->Valid()) {
            auto internal_key = block_it->key();
            ParsedInternalKey parsed_internal_key;
            if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
                s = Status::InvalidArgument("invalid key found during segment scan for GC");
                return true;
            }
            auto user_key = parsed_internal_key.user_key;
            std::unique_ptr<Iterator> leaf_it(leaf_index_->NewIterator({}));
            leaf_it->Seek(user_key);
            if (!leaf_it->Valid())
                return false;

            Slice leaf_key = leaf_it->key();
            LeafIndexEntry leaf_index_entry = leaf_it->value();

            uint32_t run_idx_in_index_entry = leaf_index_entry.GetNumMiniRuns();
            uint32_t seg_id = seg->SegmentId();
            leaf_index_entry.ForEachMiniRunIndexEntry(
                    [&run_idx_in_index_entry, run_no, seg_id](const MiniRunIndexEntry &minirun_index_entry, uint32_t idx) {
                        if (minirun_index_entry.GetSegmentNumber() == seg_id &&
                            minirun_index_entry.GetRunNumberWithinSegment() ==
                            run_no) { // Found that the index entry stored in leaf_index_ is still pointing to this run in this segment
                            run_idx_in_index_entry = idx;
                            return true;
                        }
                        return false;
                    }, LeafIndexEntry::TraversalOrder::forward);

            if (run_idx_in_index_entry == leaf_index_entry.GetNumMiniRuns()) // Stale minirun, skip it
                return false;

            SegmentBuilder *seg_builder;
            s = appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder);
            if (!s.ok()) // error, early exit
                return true;
            // Copy the entire minirun to the other segment file and update leaf_index accordingly
            s = CopyMinirunRun(leaf_key, leaf_index_entry, run_idx_in_index_entry, seg_builder);
            if (!s.ok()) // error, early exit
                return true;
            copied += run_size;
        }
        return false;
    });
    //if (copied)
        fprintf(stderr, "Copied %f%% the data from segment %d\n", (copied+0.0)/segment_size * 100, seg->SegmentId());
    return Status::OK();
}


void SilkStore::GarbageCollect() {
    MutexLock g(&GCMutex);
    std::unordered_map<std::string, std::string> leaf_index_wb;

    // Simple policy: choose the segment with maximum number of invalidated runs
    constexpr int kGCSegmentCandidateNum = 5;
    std::vector<Segment *> candidates = segment_manager_->GetMostInvalidatedSegments(kGCSegmentCandidateNum);
    if (candidates.empty())
        return;
    // Disable nested garbage collection
    bool gc_on_segment_shortage = false;
    GroupedSegmentAppender appender(1, segment_manager_, options_, gc_on_segment_shortage);
    for (auto seg : candidates) {
        GarbageCollectSegment(seg, appender);
    }

    for (auto seg: candidates) {
        segment_manager_->RemoveSegment(seg->SegmentId());
    }
}

Status SilkStore::InvalidateLeafRuns(const LeafIndexEntry & leaf_index_entry, size_t start_minirun_no, size_t end_minirun_no) {
    Status s = Status::OK();
    leaf_index_entry.ForEachMiniRunIndexEntry([&](const MiniRunIndexEntry & index_entry, uint32_t no) -> bool {
        if (start_minirun_no <= no && no <= end_minirun_no) {
            s = segment_manager_->InvalidateSegmentRun(index_entry.GetSegmentNumber(), index_entry.GetRunNumberWithinSegment());
            if (!s.ok()) {
                return true;
            }
        }
        return false;
    }, LeafIndexEntry::TraversalOrder::forward);
    return s;
}

Status SilkStore::OptimizeLeaf() {
    Log(options_.info_log, "Updating read hotness for all leaves.");
    stat_store_.UpdateReadHotness();

    if (options_.enable_leaf_read_opt == false)
        return Status::OK();
    Log(options_.info_log, "Scanning for leaves that are suitable for optimization.");
    auto it = leaf_index_->NewIterator(ReadOptions{});
    DeferCode c([it](){ delete it; });

    constexpr int kOptimizationK = 1000;
    struct HeapItem {
        double read_hotness;
        std::shared_ptr<std::string> leaf_max_key;
        std::shared_ptr<std::string> leaf_index_entry_payload;
        bool operator<(const HeapItem & rhs) const {
            return read_hotness < rhs.read_hotness;
        }
    };

    // Maintain a min-heap of kOptimizationK elements based on read-hotness
    std::priority_queue<HeapItem> candidate_heap;

    it->SeekToFirst();
    while (it->Valid()) {
        auto leaf_max_key = it->key().ToString();
        LeafIndexEntry index_entry(it->value());
        double write_hotness = stat_store_.GetWriteHotness(leaf_max_key);
        double read_hotness = stat_store_.GetReadHotness(leaf_max_key);

        if (index_entry.GetNumMiniRuns() >= options_.leaf_max_num_miniruns / 4 && read_hotness > 10) {
            //fprintf(stderr, "key %s, Rh %lf Wh %lf\n", leaf_max_key.c_str(), read_hotness, write_hotness);
            if (candidate_heap.size() < kOptimizationK) {
                candidate_heap.push(HeapItem{read_hotness, std::make_shared<std::string>(leaf_max_key), std::make_shared<std::string>(it->value().ToString())});
            } else {
                if (read_hotness > candidate_heap.top().read_hotness) {
                    candidate_heap.pop();
                    candidate_heap.push(HeapItem{read_hotness, std::make_shared<std::string>(leaf_max_key), std::make_shared<std::string>(it->value().ToString())});
                }
            }
        }
        it->Next();
    }

    std::unique_ptr<SegmentBuilder> seg_builder;
    uint32_t seg_id;
    Status s;
    std::string buf;

    if (candidate_heap.size()) {
        bool gc_on_segment_shortage = true;
        s = segment_manager_->NewSegmentBuilder(&seg_id, seg_builder, gc_on_segment_shortage);
        if (!s.ok()) {
            return s;
        }
    }

    int compacted_runs = 0;
    // Now candidate_heap contains kOptimizationK leaves with largest read-hotness and ready for optimization
    while (!candidate_heap.empty()) {
        if (seg_builder->FileSize() > options_.segment_file_size_thresh) {
            s = seg_builder->Finish();
            //fprintf(stderr, "Segment %d filled up, creating a new one\n", seg_id);
            if (!s.ok()) {
                return s;
            }
            bool gc_on_segment_shortage = true;
            s = segment_manager_->NewSegmentBuilder(&seg_id, seg_builder, gc_on_segment_shortage);
            if (!s.ok()) {
                return s;
            }
        }
        HeapItem item = candidate_heap.top(); candidate_heap.pop();
        LeafIndexEntry index_entry(*item.leaf_index_entry_payload);
        //fprintf(stderr, "optimization candidate leaf key %s, Rh %lf, compacting miniruns[%d, %d]\n", item.leaf_max_key->c_str(), item.read_hotness, 0, index_entry.GetNumMiniRuns() - 1);
        assert(seg_builder->RunStarted() == false);
        LeafIndexEntry new_index_entry = CompactLeaf(seg_builder.get(), seg_id, index_entry, s, &buf, 0, index_entry.GetNumMiniRuns() - 1);
        assert(seg_builder->RunStarted() == false);
        if (!s.ok()) {
            return s;
        }
        s = leaf_index_->Put(WriteOptions{}, Slice(*item.leaf_max_key), new_index_entry.GetRawData());
        if (!s.ok()) {
            return s;
        }
        s = InvalidateLeafRuns(index_entry, 0, index_entry.GetNumMiniRuns() - 1);
        if (!s.ok()) {
            return s;
        }
        compacted_runs += index_entry.GetNumMiniRuns();
    }
    if (seg_builder.get()) {
        return seg_builder->Finish();
    }
    if (compacted_runs)
        fprintf(stderr, "Leaf Optimization compacted %d runs\n", compacted_runs);
    return s;
}


Status SilkStore::MakeRoomInLeafLayer(WriteBatch & leaf_index_wb) {
    SequenceNumber seq_num = max_sequence_;
    mutex_.Unlock();
    ReadOptions ro;
    ro.snapshot = leaf_index_->GetSnapshot();

    // Release snapshot after the traversal is done
    DeferCode c([&ro, this](){leaf_index_->ReleaseSnapshot(ro.snapshot); mutex_.Lock(); });

    std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));

    iit->SeekToFirst();

    GroupedSegmentAppender grouped_segment_appender(1, segment_manager_, options_);
    Status s;
    int num_splits = 0;
    int self_compaction = 0;
    std::string buf, buf2, buf3, buf4, buf5, buf6;

    while (iit->Valid()  && s.ok()) {
        Slice leaf_max_key = iit->key();
        LeafIndexEntry leaf_index_entry(iit->value());

        SegmentBuilder* seg_builder = nullptr;
        s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder);
        if (!s.ok())
            return s;
        uint32_t seg_id = seg_builder->SegmentId();

        int num_miniruns = leaf_index_entry.GetNumMiniRuns();

        if (num_miniruns >= options_.leaf_max_num_miniruns
            || leaf_index_entry.GetLeafDataSize() >= options_.leaf_datasize_thresh ) {
            bool compact_all = false;
            if ( (leaf_index_entry.GetLeafDataSize() >= options_.leaf_datasize_thresh ||
                  num_leaves < allowed_num_leaves)) {
                //fprintf(stderr, "Splitting leaf with max key %s at sequence num %lu segment %d ", leaf_max_key.ToString().c_str(), seq_num, seg_id);
                s = SplitLeaf(seg_builder, seg_id, leaf_index_entry, seq_num, &buf3, &buf4, &buf5, &buf6);
                if (!s.ok()) {
                    if (s.IsSplitUnderflow()) {
                        compact_all = true;
                        //fprintf(stderr, " underflowed\n");
                        goto compaction_inside_leaf;
                    } else {
                        return s;
                    }
                } else {
                    //fprintf(stderr, "into two leaves (%s, %s) with index content(%s, %s)\n", buf3.c_str(), buf4.c_str(), LeafIndexEntry(buf5).ToString().c_str(), LeafIndexEntry(buf6).ToString().c_str());
                }
                ++num_splits;
                // Invalidate the miniruns pointed by the old leaf index entry
                s = InvalidateLeafRuns(leaf_index_entry, 0, leaf_index_entry.GetNumMiniRuns() - 1);
                if (!s.ok()) {
                    return s;
                }

                ++num_leaves;
                leaf_max_key = buf3;
                //leaf_index_entry.ForEachMiniRunIndexEntry([](const MiniRunIndexEntry &, uint32_t){return false;}, LeafIndexEntry::TraversalOrder::forward);

                //Update the index entry for the second half leaf
                // Second half
                //s = leaf_index_->Put(WriteOptions{}, Slice(buf4), Slice(buf6));
                leaf_index_wb.Put(Slice(buf3), Slice(buf5));
                leaf_index_wb.Put(Slice(buf4), Slice(buf6));
                if (!s.ok()) {
                    return s;
                }
            } else {
                compaction_inside_leaf:
                self_compaction++;
                /* Number of leaves exceeds allowable quota, try compaction inside the leaf. */
                std::pair <uint32_t, uint32_t> p = compact_all ? std::make_pair((uint32_t)0, (uint32_t)(leaf_index_entry.GetNumMiniRuns() - 1)) :ChooseLeafCompactionRunRange(leaf_index_entry);
                uint32_t start_minirun_no = p.first;
                uint32_t end_minirun_no = p.second;
                //fprintf(stderr, "Compacting leaf with max key %s, minirun range [%d, %d] segment %d\n", leaf_max_key.ToString().c_str(), start_minirun_no, end_minirun_no, seg_id);
                LeafIndexEntry new_leaf_index_entry = CompactLeaf(seg_builder, seg_id, leaf_index_entry, s, &buf3,
                                                                  start_minirun_no, end_minirun_no);
                if (!s.ok()) {
                    return s;
                }
                // Invalidate compacted runs
                s = InvalidateLeafRuns(leaf_index_entry, start_minirun_no, end_minirun_no);
                if (!s.ok()) {
                    return s;
                }

                leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
            }
        }
        iit->Next();
    }
    fprintf(stderr, "avg runsize %d, self compactions %d, num_splits %d, num_leaves %d, memtable size %lu, segments size %lu\n", imm_->ApproximateMemoryUsage() / (num_leaves == 0 ? 1 : num_leaves), self_compaction, num_splits, (num_leaves == 0 ? 1 : num_leaves), imm_->ApproximateMemoryUsage(), segment_manager_->ApproximateSize());
    return s;
}

static int num_compactions = 0;
Status SilkStore::DoCompactionWork(WriteBatch & leaf_index_wb) {
    SequenceNumber seq_num = max_sequence_;
    mutex_.Unlock();
    ReadOptions ro;
    ro.snapshot = leaf_index_->GetSnapshot();

    // Release snapshot after the traversal is done
    DeferCode c([&ro, this](){leaf_index_->ReleaseSnapshot(ro.snapshot); mutex_.Lock(); });

    std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));
    int self_compaction = 0;
    int num_leaves_snap = (num_leaves == 0 ? 1 : num_leaves);
    int num_splits = 0;
    iit->SeekToFirst();
    std::unique_ptr<Iterator> mit(imm_->NewIterator());
    mit->SeekToFirst();
    std::string buf, buf2, buf3, buf4, buf5, buf6;
    uint32_t run_no;
    Status s;

    GroupedSegmentAppender grouped_segment_appender(1, segment_manager_, options_);

    enum {
        leaf_compacted,
        leaf_splitted,
        leaf_intact
    };

    Slice next_leaf_max_key;
    Slice next_leaf_index_value;
    Slice leaf_max_key;
    while (iit->Valid() && mit->Valid() && s.ok()) {
        if (next_leaf_max_key.empty()) {
            next_leaf_max_key = iit->key();
            next_leaf_index_value = iit->value();
        }

        Slice leaf_max_key = next_leaf_max_key;
        LeafIndexEntry leaf_index_entry(next_leaf_index_value);

        SegmentBuilder* seg_builder = nullptr;
        s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder);
        if (!s.ok())
            return s;
        uint32_t seg_id = seg_builder->SegmentId();

        int leaf_state = leaf_intact;

        assert(seg_builder->RunStarted() == false);

        int minirun_key_cnt = 0;
        // Build up a minirun of key value payloads
        while (mit->Valid()) {
            Slice imm_internal_key = mit->key();
            ParsedInternalKey parsed_internal_key;
            if (!ParseInternalKey(imm_internal_key, &parsed_internal_key)) {
                s = Status::InvalidArgument("error parsing key from immutable table during compaction");
                return s;
            }
            if (this->user_comparator()->Compare(parsed_internal_key.user_key, leaf_max_key) > 0) {
                break;
            }
            if (seg_builder->RunStarted() == false) {
                s = seg_builder->StartMiniRun();
                if (!s.ok()) {
                    return s;
                }
                assert(seg_builder->RunStarted());
            }
            seg_builder->Add(mit->key(), mit->value());
            mit->Next();
            ++minirun_key_cnt;
        }

        if (seg_builder->RunStarted()) {
            s = seg_builder->FinishMiniRun(&run_no);
            if (!s.ok()) {
                return s;
            }

            // Generate an index entry for the new minirun
            buf.clear();
            MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                                                                 seg_builder->GetFinishedRunIndexBlock(),
                                                                                 seg_builder->GetFinishedRunFilterBlock(),
                                                                                 seg_builder->GetFinishedRunDataSize(),
                                                                                 &buf);

            // Update the leaf index entry
            LeafIndexEntry new_leaf_index_entry;
            //leaf_index_entry.ForEachMiniRunIndexEntry([](const MiniRunIndexEntry &, uint32_t){return false;}, LeafIndexEntry::TraversalOrder::forward);
            LeafIndexEntryBuilder::AppendMiniRunIndexEntry(leaf_index_entry, new_minirun_index_entry, &buf2,
                                                           &new_leaf_index_entry);
            //new_leaf_index_entry.ForEachMiniRunIndexEntry([](const MiniRunIndexEntry &, uint32_t){return false;}, LeafIndexEntry::TraversalOrder::forward);
            assert(leaf_index_entry.GetNumMiniRuns() + 1 == new_leaf_index_entry.GetNumMiniRuns());
            // Write out the updated entry to leaf index
            //s = leaf_index_->Put(WriteOptions{}, leaf_max_key, new_leaf_index_entry.GetRawData());
            leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
        } else {
            // Memtable has no keys intersected with this leaf
            if (leaf_index_entry.Empty()) {
                // If the leaf became empty due to self-compaction or split, remove it from the leaf index
                //leaf_index_->Delete(WriteOptions{}, leaf_max_key);
                leaf_index_wb.Delete(leaf_max_key);
                --num_leaves;
                stat_store_.DeleteLeaf(leaf_max_key.ToString());
                //fprintf(stderr, "Deleted index entry for empty leaf of key %s\n", leaf_max_key.ToString().c_str());
            }
        }

        stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);

        iit->Next();
        if (iit->Valid()) {
            next_leaf_max_key = iit->key();
            next_leaf_index_value = iit->value();
        }
    }

    while (s.ok() && mit->Valid()) {
        std::string buf, buf2;
        SegmentBuilder* seg_builder = nullptr;
        s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder);
        if (!s.ok())
            return s;
        uint32_t seg_id = seg_builder->SegmentId();

        // Memtable has keys that are greater than all the keys in leaf_index_
        // In this case, create new leaves whose runs store no more than options_.leaf_datasize_thresh bytes of data each.
        assert(seg_builder->RunStarted() == false);
        s = seg_builder->StartMiniRun();
        if (!s.ok()) {
            fprintf(stderr, s.ToString().c_str());
            return s;
        }
        size_t bytes = 0;
        int minirun_key_cnt = 0;
        while (mit->Valid()) {
            Slice imm_internal_key = mit->key();
            ParsedInternalKey parsed_internal_key;
            if (!ParseInternalKey(mit->key(), &parsed_internal_key)) {
                s = Status::InvalidArgument("error parsing key from immutable table during compaction");
                fprintf(stderr, s.ToString().c_str());
                return s;
            }
            // A leaf holds at least one key-value pair and at most options_.leaf_datasize_thresh bytes of data.
            if (minirun_key_cnt > 0 && bytes + imm_internal_key.size() + mit->value().size() >= options_.leaf_datasize_thresh * 0.95) {
                break;
            }
            bytes += imm_internal_key.size() + mit->value().size();
            leaf_max_key = parsed_internal_key.user_key;

            seg_builder->Add(imm_internal_key, mit->value());
            ++minirun_key_cnt;
            mit->Next();
        }
        uint32_t run_no;
        seg_builder->FinishMiniRun(&run_no);
        assert(seg_builder->GetFinishedRunDataSize());
        // Generate an index entry for the new minirun
        MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                                                         seg_builder->GetFinishedRunIndexBlock(),
                                                                         seg_builder->GetFinishedRunFilterBlock(),
                                                                         seg_builder->GetFinishedRunDataSize(),
                                                                         &buf);
        LeafIndexEntry new_leaf_index_entry;
        LeafIndexEntryBuilder::AppendMiniRunIndexEntry(LeafIndexEntry{}, minirun_index_entry, &buf2,
                                                       &new_leaf_index_entry);
        //new_leaf_index_entry.ForEachMiniRunIndexEntry([](const MiniRunIndexEntry &, uint32_t){return false;}, LeafIndexEntry::TraversalOrder::forward);
        //s = leaf_index_->Put(WriteOptions{}, leaf_max_key, new_leaf_index_entry.GetRawData());
        leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
        ++num_leaves;
        stat_store_.NewLeaf(leaf_max_key.ToString());
        stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);
    }
    //fprintf(stderr, "Background compaction finished, last segment %d\n", seg_id);
    //fprintf(stderr, "avg runsize %d, self compactions %d, num_splits %d, num_leaves %d, memtable size %lu, segments size %lu\n", imm_->ApproximateMemoryUsage() / num_leaves_snap, self_compaction, num_splits, num_leaves_snap, imm_->ApproximateMemoryUsage(), segment_manager_->ApproximateSize());
    ++num_compactions;
    return s;
}

// Perform a merge between leaves and the immutable memtable.
// Single threaded version.
void SilkStore::BackgroundCompaction() {
    mutex_.Unlock();
    while (options_.maximum_segments_storage_size && segment_manager_->ApproximateSize() >= options_.segments_storage_size_gc_threshold * options_.maximum_segments_storage_size) {
        this->GarbageCollect();
    }
    mutex_.Lock();
    WriteBatch leaf_index_wb;
    Status s = MakeRoomInLeafLayer(leaf_index_wb);
    if (!s.ok()) {
        Log(options_.info_log, "MakeRoomInLeafLayer failed: %s\n", s.ToString().c_str());
        bg_error_ = s;
        return;
    }
    s = leaf_index_->Write({}, &leaf_index_wb);
    if (!s.ok()) {
        bg_error_ = s;
        Log(options_.info_log, "leaf_index_->Write failed: %s\n", s.ToString().c_str());
        return;
    }
    leaf_index_wb.Clear();
    s = DoCompactionWork(leaf_index_wb);

    if (!s.ok()) {
        Log(options_.info_log, "DoCompactionWork failed: %s\n", s.ToString().c_str());
        bg_error_ = s;
    } else {
        mutex_.Unlock();
        s = leaf_index_->Write({}, &leaf_index_wb);
        mutex_.Lock();
        if (!s.ok()) {
            bg_error_ = s;
            Log(options_.info_log, "DoCompactionWork failed: %s\n", s.ToString().c_str());
            return ;
        }
        // Save a new Current File
        SetCurrentFileWithLogNumber(env_, dbname_, logfile_number_);
        // Commit to the new state

        imm_->Unref();
        imm_ = nullptr;
        has_imm_.Release_Store(nullptr);
    }
}

Status DestroyDB(const std::string& dbname, const Options& options) {
    Status result = leveldb::DestroyDB(dbname + "/leaf_index", options);
    if (result.ok() == false)
        return result;
    Env* env = options.env;
    std::vector<std::string> filenames;
    result = env->GetChildren(dbname, &filenames);
    if (!result.ok()) {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    result = env->LockFile(lockname, &lock);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseSilkstoreFileName(filenames[i], &number, &type) &&
                type != kDBLockFile) {  // Lock file will be deleted at end
                Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }
            }
        }
        env->UnlockFile(lock);  // Ignore error since state is already gone
        env->DeleteFile(lockname);
        env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    }
    return result;
}

}  // namespace silkstore
}  // namespace leveldb