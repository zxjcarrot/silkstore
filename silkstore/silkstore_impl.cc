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
#include "leveldb/nvm_write_batch.h"
#include "silkstore/silkstore_impl.h"
#include "silkstore/silkstore_iter.h"
#include "silkstore/util.h"
#include "util/histogram.h"
#include <cmath>
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
          logfile_(nullptr),
          logfile_number_(0),
          log_(nullptr),
          max_sequence_(0),
          memtable_capacity_(options_.write_buffer_size),
          seed_(0),
          tmp_batch_(new WriteBatch),
          background_compaction_scheduled_(false),
          leaf_optimization_func_([]() {}),
          manual_compaction_(nullptr) {
    nvm_manager_ = new NvmManager(raw_options.nvm_file, raw_options.nvm_size);           
    nvm_log_ = nvm_manager_->initLog();      
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
    // if (imm_ != nullptr) imm_->Unref();
    while (!imm_.empty()){
        imm_.front()->Unref();
        imm_.pop_front();
    }
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
    if (nvm_manager_){
        delete nvm_manager_;
    }
}

Status SilkStore::OpenIndex(const Options &index_options) {
    assert(leaf_index_ == nullptr);
    Status s = DB::Open(index_options,  "/mnt/myPMem/leaf_index", &leaf_index_);
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
    NvmemTable *mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) &&
           status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(
                    record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
            nvm_log_->reset();            
            mem = new NvmemTable(internal_comparator_, nullptr, nvm_manager_->allocate(MB * 100), nvm_log_);
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
            nvm_log_->reset();            
            mem_ = new NvmemTable(internal_comparator_, nullptr, nvm_manager_->allocate(MB * 100), nvm_log_);
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
    s = SegmentManager::OpenManager(this->options_, dbname_, &segment_manager_,
                                    std::bind(&SilkStore::GarbageCollect, this));
    if (!s.ok()) return s;

    s = LeafStore::Open(segment_manager_, leaf_index_, options_, internal_comparator_.user_comparator(), &leaf_store_);
    if (!s.ok()) return s;

    std::string current_content;
    s = ReadFileToString(env_, CurrentFilename(dbname_), &current_content);
    if (s.IsNotFound()) {
        // new db
        nvm_log_->reset();            
        mem_ = new NvmemTable(internal_comparator_, nullptr, nvm_manager_->allocate(MB * 100), nvm_log_);
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

   /*  leaf_optimization_func_ = [this]() {
        this->OptimizeLeaf();
        env_->ScheduleDelayedTask(leaf_optimization_func_, LeafStatStore::read_interval_in_micros);
    };
    env_->ScheduleDelayedTask(leaf_optimization_func_, LeafStatStore::read_interval_in_micros); */
    return s;
}

Status SilkStore::TEST_CompactMemTable() {
    // nullptr batch means just wait for earlier writes to be done
    Status s = Write(WriteOptions(), nullptr);
    if (s.ok()) {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (!imm_.empty() && bg_error_.ok()) {
            background_work_finished_signal_.Wait();
        }
        if (!imm_.empty() ) {
            s = bg_error_;
        }
    }
    return s;
}

// Convenience methods
Status SilkStore::Put(const WriteOptions &option, const Slice &key, const Slice &value) {
   // fprintf(stderr, "put key: %s, seqnum: %lu\n", key.ToString().c_str(), max_sequence_);
   // return DB::Put(o, key, val);
    WriteBatch batch;
    batch.Put(key, value);
    return Write(option, &batch);
}

Status SilkStore::Delete(const WriteOptions &options, const Slice &key) {
   // return DB::Delete(options, key);
    WriteBatch batch;
    batch.Delete(key);
    return Write(options, &batch);
}

static void SilkStoreNewIteratorCleanup(void *arg1, void *arg2) {
    static_cast<MemTable *>(arg1)->Unref();
    if (arg2) static_cast<MemTable *>(arg2)->Unref();
}

const Snapshot *SilkStore::GetSnapshot() {
    MutexLock l(&mutex_);
    return leaf_index_->GetSnapshot();
}

void SilkStore::ReleaseSnapshot(const Snapshot *snapshot) {
    MutexLock l(&mutex_);
    return leaf_index_->ReleaseSnapshot(snapshot);
}

Iterator *SilkStore::NewIterator(const ReadOptions &ropts) {
    MutexLock l(&mutex_);
    SequenceNumber seqno = ropts.snapshot ? dynamic_cast<const SnapshotImpl *>(ropts.snapshot)->sequence_number()
                                          : max_sequence_;
    // Collect together all needed child iterators
    std::vector<Iterator *> list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    if(!imm_.empty()){
        for( auto it: imm_){
            list.push_back(it->NewIterator());
            it->Ref();
        }
    }
    list.push_back(leaf_store_->NewIterator(ropts));
    Iterator *internal_iter =
            NewMergingIterator(&internal_comparator_, &list[0], list.size());
    internal_iter->RegisterCleanup(SilkStoreNewIteratorCleanup, mem_, nullptr);

    if(!imm_.empty()){
        for( auto it: imm_){
            internal_iter->RegisterCleanup(SilkStoreNewIteratorCleanup, it, nullptr);
        }
    }
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
        } else if ( !imm_.empty()  &&  imm_.size() >= options_.max_imm_num * 5) {
            Log(options_.info_log, "Current memtable full;Compaction ongoing; waiting...\n");
            std::cout<< "Current memtable full;Compaction ongoing; waiting...\n";
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

            imm_.push_back(mem_);
            has_imm_.Release_Store(imm_.back());            
            
            size_t old_memtable_capacity = memtable_capacity_;
            size_t new_memtable_capacity =
                    (memtable_capacity_ + segment_manager_->ApproximateSize()) / options_.memtbl_to_L0_ratio;
            new_memtable_capacity = std::min(options_.max_memtbl_capacity,
                                             std::max(options_.write_buffer_size, new_memtable_capacity));
            Log(options_.info_log, " ### new memtable capacity %lu\n", new_memtable_capacity);
         //   std::cout << "new_memtable_capacity:" << new_memtable_capacity << "\n";

            memtable_capacity_ = new_memtable_capacity;
            allowed_num_leaves = std::ceil(new_memtable_capacity / (options_.storage_block_size + 0.0));
            DynamicFilter *dynamic_filter = nullptr;
            if (options_.use_memtable_dynamic_filter) {
                size_t imm_num_entries = imm_.back()->NumEntries();
                size_t new_memtable_capacity_num_entries =
                        imm_num_entries * std::ceil(new_memtable_capacity / (old_memtable_capacity + 0.0));
                assert(new_memtable_capacity_num_entries);
                dynamic_filter = NewDynamicFilterBloom(new_memtable_capacity_num_entries,
                                                       options_.memtable_dynamic_filter_fp_rate);
            }
            nvm_log_->reset();
            mem_ = new NvmemTable(internal_comparator_, dynamic_filter,
                        nvm_manager_->allocate( new_memtable_capacity + 1000), nvm_log_);
            mem_->Ref();
            force = false;   // Do not force another compaction if have room
            MaybeScheduleCompaction();
        }
    }

    return s;
}



Status SilkStore::NvmMakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!nvmwriters_.empty());
    bool allow_delay = !force;
    Status s;
    while (true) {
        size_t memtbl_size = mem_->ApproximateMemoryUsage();
        if (!force && (memtbl_size <= memtable_capacity_)) {
            break;
        } else if ( !imm_.empty()  &&  imm_.size() >= options_.max_imm_num * 5) {
            Log(options_.info_log, "Current memtable full;Compaction ongoing; waiting...\n");
            std::cout<< "Current memtable full;Compaction ongoing; waiting...\n";
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
            imm_.push_back(mem_);
            has_imm_.Release_Store(imm_.back());
            size_t old_memtable_capacity = memtable_capacity_;
            size_t new_memtable_capacity =
                    (memtable_capacity_ + segment_manager_->ApproximateSize()) / options_.memtbl_to_L0_ratio;
            new_memtable_capacity = std::min(options_.max_memtbl_capacity,
                                             std::max(options_.write_buffer_size, new_memtable_capacity));
            Log(options_.info_log, " ### new memtable capacity %lu\n", new_memtable_capacity);
         //   std::cout << "new_memtable_capacity:" << new_memtable_capacity << "\n";

            memtable_capacity_ = new_memtable_capacity;
            allowed_num_leaves = std::ceil(new_memtable_capacity / (options_.storage_block_size + 0.0));
            DynamicFilter *dynamic_filter = nullptr;
            if (options_.use_memtable_dynamic_filter) {
                size_t imm_num_entries = imm_.back()->NumEntries();
                size_t new_memtable_capacity_num_entries =
                        imm_num_entries * std::ceil(new_memtable_capacity / (old_memtable_capacity + 0.0));
                assert(new_memtable_capacity_num_entries);
                dynamic_filter = NewDynamicFilterBloom(new_memtable_capacity_num_entries,
                                                       options_.memtable_dynamic_filter_fp_rate);
            }
            nvm_log_->reset();
            mem_ = new NvmemTable(internal_comparator_, dynamic_filter,
                        nvm_manager_->allocate( new_memtable_capacity + 10240), nvm_log_);
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
    } else if ( imm_.empty()  &&
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


struct SilkStore::NvmWriter {
    Status status;
    NvmWriteBatch *batch;
    bool sync;
    bool done;
    port::CondVar cv;
    explicit NvmWriter(port::Mutex *mu) : cv(mu) {}
};





Status SilkStore::NvmWrite(const WriteOptions &options, NvmWriteBatch *my_batch) {
    NvmWriter w(&mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    nvmwriters_.push_back(&w);
    while (!w.done && &w != nvmwriters_.front()) {
        w.cv.Wait();
    }
    if (w.done) {
        return w.status;
    }

    // May temporarily unlock and wait.
    Status status = NvmMakeRoomForWrite(my_batch == nullptr);
    uint64_t last_sequence = max_sequence_;
    NvmWriter *last_writer = &w;
    if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
        NvmWriteBatch *updates = NvmBuildBatchGroup(&last_writer);
        // updates->SetSequence(last_sequence + 1);
        // last_sequence += updates->Counter();

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        {
            mutex_.Unlock();
           /*  status = log_->AddRecord(WriteBatchInternal::Contents(updates));
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
             */

            // Using Nvm to insert data without log
            status = mem_->AddBatch(updates);
            
            mutex_.Lock();
           /*  if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                //RecordBackgroundError(status);
                bg_error_ = status;
            } */
        }
        if (updates == nvm_tmp_batch_) {
            std::cout<<"nvm_tmp_batch_->Clear()\n";
            nvm_tmp_batch_->Clear();
        }
        max_sequence_ = last_sequence;
    }

    while (true) {
        NvmWriter *ready = nvmwriters_.front();
        nvmwriters_.pop_front();
        // std::cout << "nvmwriters_.pop_front()\n";
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
        size_t nums =  WriteBatchInternal::Count(updates);
        last_sequence += nums;

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        {
            mutex_.Unlock();
           /*  status = log_->AddRecord(WriteBatchInternal::Contents(updates));
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
             */
            // Using Nvm to insert data without log
            // status = mem_->AddBatch(updates);
            status = WriteBatchInternal::InsertInto(updates, mem_);
          //  mem_->AddCounter(nums);

          //  std::cout <<  mem_->GetCounter() << "\n";
            mutex_.Lock();
           /*  if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                //RecordBackgroundError(status);
                bg_error_ = status;
            } */
        }
        if (updates == tmp_batch_) {
            tmp_batch_->Clear();
            std::cout<<"tmp_batch_->Clear()\n";
        }

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
        DeferCode c([it]() { delete it; });
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
        DeferCode c([it]() { delete it; });
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
        DeferCode c([it]() { delete it; });
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
        if (!imm_.empty()) {
            res += imm_.back()->Searches();
        }
        *value = std::to_string(res);
        return true;
    } else if (property.ToString() == "silkstore.gcstat") {
        *value = "\ntime spent in gc: " + std::to_string(stats_.time_spent_gc) + "us\n";
        return true;
    } else if (property.ToString() == "silkstore.segment_util") {
        *value = this->SegmentsSpaceUtilityHistogram();
        return true;
    } else if (property.ToString() == "silkstore.stats") {

        char buf[1000];
        snprintf(buf, sizeof(buf), "\nbytes rd %lu\n"
                                   "bytes wt %lu\n"
                                   "bytes rd gc %lu\n"
                                   "bytes rd gc %lu (Actual)\n"
                                   "bytes wt gc %lu\n"
                                   "# miniruns checked for gc %lu\n"
                                   "# miniruns queried for gc %lu\n",
                 stats_.bytes_read,
                 stats_.bytes_written,
                 stats_.gc_bytes_read_unopt,
                 stats_.gc_bytes_read,
                 stats_.gc_bytes_written,
                 stats_.gc_miniruns_total,
                 stats_.gc_miniruns_queried);
        *value = buf;
        std::string leaf_index_stats;
        leaf_index_->GetProperty("leveldb.stats", &leaf_index_stats);
        value->append(leaf_index_stats);
        return true;
    } else if (property.ToString() == "silkstore.write_volume") {
        *value = std::to_string(stats_.bytes_written);
        return true;
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
    NvmemTable *mem = mem_;
    mem->Ref();
   if (!imm_.empty()) {
        for(auto it : imm_){
           it->Ref();
        }
    }           
    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        // First look in the memtable, then in the immutable memtable (if any).
        LookupKey lkey(key, snapshot);
        bool find = false;
        if (mem->Get(lkey, value, &s)) {
            // Done
           find = true;
        } else if (!imm_.empty()) {
            for(auto it = imm_.rbegin() ;it != imm_.rend(); it++){
                 if ((*it)->Get(lkey, value, &s)){
                     find = true;
                     break;
                 }
            }            
        }
        if (!find) {
            s = leaf_store_->Get(options, lkey, value, stat_store_);
        }
        mutex_.Lock();
    }

//    if (have_stat_update && current->UpdateStats(stats)) {
//        MaybeScheduleCompaction();
//    }
    mem->Unref();
    if (!imm_.empty()) {
        for(auto it : imm_){
           it->Unref();
        }
    }           
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




// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
NvmWriteBatch *SilkStore::NvmBuildBatchGroup(NvmWriter **last_writer) {
    mutex_.AssertHeld();
    assert(!nvmwriters_.empty());
    NvmWriter *first = nvmwriters_.front();
    NvmWriteBatch *result = first->batch;
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
    std::deque<NvmWriter *>::iterator iter = nvmwriters_.begin();
    ++iter;  // Advance past "first"
    for (; iter != nvmwriters_.end(); ++iter) {
        NvmWriter *w = *iter;
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
                result = nvm_tmp_batch_;
               // assert(WriteBatchInternal::Count(result) == 0);
              //  assert(result->Counter() == 0);
                result->Append(first->batch);
            }
            result->Append(w->batch);
            //WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}


class GroupedSegmentAppender {
public:
    GroupedSegmentAppender(int num_groups, SegmentManager *segment_manager, const Options &options,
                           bool gc_on_segment_shortage = true) : builders(num_groups), segment_manager(segment_manager),
                                                                 options(options),
                                                                 gc_on_segment_shortage(gc_on_segment_shortage) {}

    // Make sure the segment that is being built by a group has enough space.
    // If not, finish off the old segment and create a new segment.
    // Return the builder of the designated group.
    Status MakeRoomForGroupAndGetBuilder(uint32_t group_id, SegmentBuilder **builder_ptr, bool &switched_segment) {
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
        switched_segment = true;
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
    std::vector<SegmentBuilder *> builders;
    SegmentManager *segment_manager;
    Options options;
    bool gc_on_segment_shortage;
};

std::pair<uint32_t, uint32_t> SilkStore::ChooseLeafCompactionRunRange(const LeafIndexEntry &leaf_index_entry) {
    // TODO: come up with a better approach
    uint32_t num_runs = leaf_index_entry.GetNumMiniRuns();
    assert(num_runs > 1);
    return {num_runs - 2, num_runs - 1};
}


LeafIndexEntry
SilkStore::CompactLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s,
                       std::string *buf, uint32_t start_minirun_no, uint32_t end_minirun_no,
                       const Snapshot *leaf_index_snap) {
    buf->clear();
    bool cover_whole_range = end_minirun_no - start_minirun_no + 1 == leaf_index_entry.GetNumMiniRuns();
    ReadOptions ropts;
    ropts.snapshot = leaf_index_snap;
    Iterator *it = leaf_store_->NewIteratorForLeaf(ropts, leaf_index_entry, s, start_minirun_no,
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
                                 SegmentBuilder *target_seg_builder, WriteBatch & leaf_index_wb) {
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
    leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
    return s;
}

Status SilkStore::GarbageCollectSegment(Segment *seg, GroupedSegmentAppender &appender, WriteBatch & leaf_index_wb) {
    Status s;
    size_t copied = 0;
    size_t segment_size = seg->SegmentSize();

    seg->ForEachRun([&, this](int run_no, MiniRunHandle run_handle, size_t run_size, bool valid) {
        stats_.AddGCUnoptStats(std::max(options_.block_size, run_handle.last_block_handle.size()));
        if (valid == false) { // Skip invalidated runs
            stats_.AddGCMiniRunStats(0, 1);
            return false;
        }
        stats_.AddGCMiniRunStats(1, 1);
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

        // Read the last block aligned by options_.block_size
        stats_.AddGCStats(std::max(options_.block_size, last_block_handle.size()), 0);
        stats_.Add(std::max(options_.block_size, last_block_handle.size()), 0);
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
                    [&run_idx_in_index_entry, run_no, seg_id](const MiniRunIndexEntry &minirun_index_entry,
                                                              uint32_t idx) {
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
            bool switched_segment = false;
            s = appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder, switched_segment);
            if (!s.ok()) // error, early exit
                return true;
            // Copy the entire minirun to the other segment file and update leaf_index accordingly
            s = CopyMinirunRun(leaf_key, leaf_index_entry, run_idx_in_index_entry, seg_builder, leaf_index_wb);
            if (!s.ok()) // error, early exit
                return true;
            // Read from the old leaf
            // Write to the new leaf
            stats_.Add(leaf_index_entry.GetLeafDataSize(), leaf_index_entry.GetLeafDataSize());
            stats_.AddGCStats(leaf_index_entry.GetLeafDataSize(), leaf_index_entry.GetLeafDataSize());
            copied += run_size;
        }
        return false;
    });
    //if (copied)
    //fprintf(stderr, "Copied %f%% the data from segment %d of size %lu\n", (copied+0.0)/segment_size * 100, seg->SegmentId(), segment_size);
    return Status::OK();
}


std::string SilkStore::SegmentsSpaceUtilityHistogram() {
    MutexLock g(&GCMutex);
    Histogram hist;
    Status s;
    hist.Clear();
    size_t total_segment_size = 0;
    size_t total_valid_size = 0;
    segment_manager_->ForEachSegment([&, this](Segment *seg) {
        size_t seg_size = seg->SegmentSize();
        total_segment_size += seg_size;
        size_t valid_size = 0;
        bool error = false;
        seg->ForEachRun([&, this](int run_no, MiniRunHandle run_handle, size_t run_size, bool valid) {
            if (valid == false) { // Skip invalidated runs
                return false;
            }
            // Maybe valid, we'll see.

            // We take out the first key of the last block in the run to query leaf_index for validness
            MiniRun *run;
            Block index_block(BlockContents({Slice(), false, false}));
            s = seg->OpenMiniRun(run_no, index_block, &run);
            if (!s.ok()) {
                // error, early exit
                error = true;
                return true;
            }


            DeferCode c([run]() { delete run; });
            BlockHandle last_block_handle = run_handle.last_block_handle;

            std::unique_ptr<Iterator> block_it(
                    run->NewIteratorForOneBlock({}, last_block_handle));

            // Read the last block aligned by options_.block_size
            block_it->SeekToFirst();
            if (block_it->Valid()) {
                auto internal_key = block_it->key();
                ParsedInternalKey parsed_internal_key;
                if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
                    s = Status::InvalidArgument("invalid key found during segment scan for GC");
                    error = true;
                    return true;
                }
                auto user_key = parsed_internal_key.user_key;
                std::unique_ptr<Iterator> leaf_it(leaf_index_->NewIterator({}));
                leaf_it->Seek(user_key);
                if (!leaf_it->Valid())
                    return false;

                LeafIndexEntry leaf_index_entry = leaf_it->value();

                uint32_t run_idx_in_index_entry = leaf_index_entry.GetNumMiniRuns();
                uint32_t seg_id = seg->SegmentId();
                leaf_index_entry.ForEachMiniRunIndexEntry(
                        [&run_idx_in_index_entry, run_no, seg_id](const MiniRunIndexEntry &minirun_index_entry,
                                                                  uint32_t idx) {
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

                valid_size += run_size;
            }
            return false;
        });
        if (error == false) {
            assert(valid_size <= seg_size);
            double util = (valid_size + 0.0) / seg_size;
            hist.Add(util * 100);
            total_valid_size += valid_size;
        }

    });
    return hist.ToString() + "\ntotal_valid_size: " + std::to_string(total_valid_size) + "\ntotal_segment_size : " +
           std::to_string(total_segment_size) + "\n";
}

int SilkStore::GarbageCollect() {
    MutexLock g(&GCMutex);

    WriteBatch leaf_index_wb;
    // Simple policy: choose the segment with maximum number of invalidated runs
    constexpr int kGCSegmentCandidateNum = 5;
    std::vector<Segment *> candidates = segment_manager_->GetMostInvalidatedSegments(kGCSegmentCandidateNum);
    if (candidates.empty())
        return 0;
    // Disable nested garbage collection
    bool gc_on_segment_shortage = false;
    GroupedSegmentAppender appender(1, segment_manager_, options_, gc_on_segment_shortage);
    for (auto seg : candidates) {
        GarbageCollectSegment(seg, appender, leaf_index_wb);
    }
    if (leaf_index_wb.ApproximateSize()) {
        leaf_index_->Write({}, &leaf_index_wb);
    }
    for (auto seg: candidates) {
        segment_manager_->RemoveSegment(seg->SegmentId());
    }
    return candidates.size();
}

Status
SilkStore::InvalidateLeafRuns(const LeafIndexEntry &leaf_index_entry, size_t start_minirun_no, size_t end_minirun_no) {
    Status s = Status::OK();
    leaf_index_entry.ForEachMiniRunIndexEntry([&](const MiniRunIndexEntry &index_entry, uint32_t no) -> bool {
        if (start_minirun_no <= no && no <= end_minirun_no) {
            s = segment_manager_->InvalidateSegmentRun(index_entry.GetSegmentNumber(),
                                                       index_entry.GetRunNumberWithinSegment());
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

    constexpr int kOptimizationK = 1000;
    struct HeapItem {
        double read_hotness;
        std::shared_ptr<std::string> leaf_max_key;

        bool operator<(const HeapItem &rhs) const {
            return read_hotness < rhs.read_hotness;
        }
    };
    MutexLock g(&GCMutex);

    auto leaf_index_snapshot = leaf_index_->GetSnapshot();
    DeferCode c([this, &leaf_index_snapshot]() { leaf_index_->ReleaseSnapshot(leaf_index_snapshot); });

    // Maintain a min-heap of kOptimizationK elements based on read-hotness
    std::priority_queue<HeapItem> candidate_heap;

    stat_store_.ForEachLeaf(
            [this, &candidate_heap](const std::string &leaf_max_key, const LeafStatStore::LeafStat &stat) {
                double read_hotness = stat.read_hotness;
                if (stat.num_runs >= options_.leaf_max_num_miniruns / 4 && read_hotness > 0) {
                    if (candidate_heap.size() < kOptimizationK) {
                        candidate_heap.push(HeapItem{read_hotness, std::make_shared<std::string>(leaf_max_key)});
                    } else {
                        if (read_hotness > candidate_heap.top().read_hotness) {
                            candidate_heap.pop();
                            candidate_heap.push(HeapItem{read_hotness, std::make_shared<std::string>(leaf_max_key)});
                        }
                    }
                }
            });

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
    WriteBatch leaf_index_wb;
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
            s = leaf_index_->Write(WriteOptions{}, &leaf_index_wb);
            if (!s.ok()) {
                return s;
            }
            leaf_index_wb.Clear();
        }
        HeapItem item = candidate_heap.top();
        candidate_heap.pop();
        ReadOptions ropts;
        ropts.snapshot = leaf_index_snapshot;
        std::string leaf_index_entry_payload;
        s = leaf_index_->Get(ropts, *item.leaf_max_key, &leaf_index_entry_payload);
        if (!s.ok())
            continue;
        LeafIndexEntry index_entry(leaf_index_entry_payload);
        //fprintf(stderr, "optimization candidate leaf key %s, Rh %lf, compacting miniruns[%d, %d]\n", item.leaf_max_key->c_str(), item.read_hotness, 0, index_entry.GetNumMiniRuns() - 1);
        assert(seg_builder->RunStarted() == false);
        LeafIndexEntry new_index_entry = CompactLeaf(seg_builder.get(), seg_id, index_entry, s, &buf, 0,
                                                     index_entry.GetNumMiniRuns() - 1, leaf_index_snapshot);
        assert(seg_builder->RunStarted() == false);
        if (!s.ok()) {
            return s;
        }
        leaf_index_wb.Put(Slice(*item.leaf_max_key), new_index_entry.GetRawData());
        s = InvalidateLeafRuns(index_entry, 0, index_entry.GetNumMiniRuns() - 1);
        if (!s.ok()) {
            return s;
        }
        compacted_runs += index_entry.GetNumMiniRuns();
        stat_store_.UpdateLeafNumRuns(*item.leaf_max_key, 1);
    }
    if (compacted_runs)
        fprintf(stderr, "Leaf Optimization compacted %d runs\n", compacted_runs);
    if (seg_builder.get()) {
        return seg_builder->Finish();
    }
    if (leaf_index_wb.ApproximateSize()) {
        return leaf_index_->Write(WriteOptions{}, &leaf_index_wb);
    }
    return s;
}

constexpr size_t kLeafIndexWriteBufferMaxSize = 4 * 1024 * 1024;

Status SilkStore::MakeRoomInLeafLayer(bool force) {
    WriteBatch leaf_index_wb;
    SequenceNumber seq_num = max_sequence_;
    mutex_.Unlock();
    restart:
    {
        ReadOptions ro;
        ro.snapshot = leaf_index_->GetSnapshot();
        // Release snapshot after the traversal is done
        DeferCode c([&ro, this]() {
            leaf_index_->ReleaseSnapshot(ro.snapshot);
            mutex_.Lock();
        });

        std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));

        iit->SeekToFirst();

        GroupedSegmentAppender grouped_segment_appender(1, segment_manager_, options_);
        Status s;
        int num_splits = 0;
        int self_compaction = 0;

        auto SplitLeaf = [&grouped_segment_appender, &leaf_index_wb, this](const LeafIndexEntry &leaf_index_entry,
                                                                           SequenceNumber seq_num,
                                                                           std::vector<std::string> &max_keys,
                                                                           std::vector<std::string> &max_key_index_entry_bufs) {
            Status s;
            /* We use DBIter to get the most recent non-deleted keys. */
            auto it = dynamic_cast<silkstore::DBIter *>(leaf_store_->NewDBIterForLeaf(ReadOptions{}, leaf_index_entry,
                                                                                      s,
                                                                                      user_comparator(), seq_num));

            DeferCode c([it]() { delete it; });

            it->SeekToFirst();

            size_t bytes_current_leaf = 0;

            SegmentBuilder *seg_builder = nullptr;
            auto AssignSegmentBuilder = [&seg_builder, &grouped_segment_appender, &leaf_index_wb, this]() {
                bool switched_segment = false;
                Status s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder, switched_segment);
                if (!s.ok())
                    return s;

                if (switched_segment && leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
                    // If all previous segments are built successfully and
                    // the leaf_index write buffer exceeds the threshold,
                    // write it down to leaf_index_ to keep the memory footprint small.
                    s = leaf_index_->Write({}, &leaf_index_wb);
                    if (!s.ok())
                        return s;
                    leaf_index_wb.Clear();
                }
                return Status::OK();
            };


            std::string max_key;
            std::string max_key_index_entry_buf;
            while (it->Valid()) {
                if (seg_builder == nullptr) {
                    s = AssignSegmentBuilder();
                    if (!s.ok())
                        return s;
                    seg_builder->StartMiniRun();
                }
                bytes_current_leaf += it->internal_key().size() + it->value().size();
                /*
                 * Since splitting a leaf should preserve the sequence numbers of the most recent non-deleted keys,
                 * we modified DBIter to provide access to its internal key representation.
                 * */
                seg_builder->Add(it->internal_key(), it->value());
                max_key = it->key().ToString();
                it->Next();
                if (bytes_current_leaf >= options_.leaf_datasize_thresh / 2 || it->Valid() == false) {
                    uint32_t run_no;
                    seg_builder->FinishMiniRun(&run_no);
                    max_key_index_entry_buf.clear();
                    std::string buf;
                    MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(seg_builder->SegmentId(), run_no,
                                                                                     seg_builder->GetFinishedRunIndexBlock(),
                                                                                     seg_builder->GetFinishedRunFilterBlock(),
                                                                                     seg_builder->GetFinishedRunDataSize(),
                                                                                     &buf);
                    LeafIndexEntry new_leaf_index_entry;
                    LeafIndexEntryBuilder::AppendMiniRunIndexEntry(LeafIndexEntry{}, minirun_index_entry,
                                                                   &max_key_index_entry_buf, &new_leaf_index_entry);
                    max_keys.push_back(max_key);
                    max_key_index_entry_bufs.push_back(max_key_index_entry_buf);
                    if (it->Valid() == true) {
                        // If there are more key values left, keep working.
                        s = AssignSegmentBuilder();
                        if (!s.ok())
                            return s;
                        seg_builder->StartMiniRun();
                    }
                    bytes_current_leaf = 0;
                }
            }
            return Status::OK();
        };

        while (iit->Valid() && s.ok()) {
            Slice leaf_max_key = iit->key();
            LeafIndexEntry leaf_index_entry(iit->value());

            int num_miniruns = leaf_index_entry.GetNumMiniRuns();

            if (force ||
                num_miniruns >= options_.leaf_max_num_miniruns
                //|| leaf_index_entry.GetLeafDataSize() >= options_.leaf_datasize_thresh
                    ) {

                SegmentBuilder *seg_builder = nullptr;
                bool switched_segment = false;
                s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder, switched_segment);
                if (!s.ok())
                    return s;

                if (switched_segment && leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
                    // If all previous segments are built successfully and
                    // the leaf_index write buffer exceeds the threshold,
                    // write it down to leaf_index_ to keep the memory footprint small.
                    s = leaf_index_->Write({}, &leaf_index_wb);
                    if (!s.ok())
                        return s;
                    leaf_index_wb.Clear();
                }

                uint32_t seg_id = seg_builder->SegmentId();

                //if ((leaf_index_entry.GetLeafDataSize() >= options_.leaf_datasize_thresh)) {
                    //fprintf(stderr, "Splitting leaf with max key %s at sequence num %lu segment %d\n",
                    //        leaf_max_key.ToString().c_str(), seq_num, seg_id);
                    std::vector<std::string> max_keys;
                    std::vector<std::string> max_key_index_entry_bufs;
                    s = SplitLeaf(leaf_index_entry, seq_num, max_keys, max_key_index_entry_bufs);
                    assert(max_keys.size() == max_key_index_entry_bufs.size());
                    if (!s.ok())
                        return s;
                    ++num_splits;
                    // Invalidate the miniruns pointed by the old leaf index entry
                    s = InvalidateLeafRuns(leaf_index_entry, 0, leaf_index_entry.GetNumMiniRuns() - 1);
                    if (!s.ok()) {
                        return s;
                    }

                    //Update the index entries
                    stat_store_.SplitLeaf(leaf_max_key.ToString(), max_keys);
                    leaf_index_wb.Delete(leaf_max_key);
                    --num_leaves;
                    for (size_t i = 0; i < max_keys.size(); ++i) {
                        leaf_index_wb.Put(Slice(max_keys[i]), Slice(max_key_index_entry_bufs[i]));
                        stat_store_.UpdateLeafNumRuns(max_keys[i], 1);
                    }
                    num_leaves += max_keys.size();
//                } else {
//                    self_compaction++;
//                    /* Number of leaves exceeds allowable quota, try compaction inside the leaf. */
//                    std::pair<uint32_t, uint32_t> p = ChooseLeafCompactionRunRange(leaf_index_entry);
//                    uint32_t start_minirun_no = p.first;
//                    uint32_t end_minirun_no = p.second;
//                    //fprintf(stderr, "Compacting leaf with max key %s, minirun range [%d, %d] segment %d\n", leaf_max_key.ToString().c_str(), start_minirun_no, end_minirun_no, seg_id);
//                    std::string buf;
//                    LeafIndexEntry new_leaf_index_entry = CompactLeaf(seg_builder, seg_id, leaf_index_entry, s, &buf,
//                                                                      start_minirun_no, end_minirun_no);
//                    if (!s.ok()) {
//                        return s;
//                    }
//                    // Invalidate compacted runs
//                    s = InvalidateLeafRuns(leaf_index_entry, start_minirun_no, end_minirun_no);
//                    if (!s.ok()) {
//                        return s;
//                    }
//
//                    leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
//                    stat_store_.UpdateLeafNumRuns(leaf_max_key.ToString(), new_leaf_index_entry.GetNumMiniRuns());
//                }
                // Read all data and merge them, then write all out
                stats_.Add(leaf_index_entry.GetLeafDataSize(), leaf_index_entry.GetLeafDataSize());
            }
            // Record the data read from leaf_index as well
            stats_.Add(iit->key().size() + iit->value().size(), 0);
            iit->Next();
        }


        if (!s.ok()) {
            Log(options_.info_log, "MakeRoomInLeafLayer failed: %s\n", s.ToString().c_str());
            return s;
        }
        if (leaf_index_wb.ApproximateSize()) {
            s = leaf_index_->Write({}, &leaf_index_wb);
            if (!s.ok()) {
                Log(options_.info_log, "leaf_index_->Write failed: %s\n", s.ToString().c_str());
                return s;
            }
        }

        //fprintf(stderr, "avg runsize %d, self compactions %d, num_splits %d, num_leaves %d, 
        // memtable size %lu, segments size %lu\n", imm_->ApproximateMemoryUsage() / (num_leaves == 0 ? 1 : num_leaves), self_compaction, num_splits, (num_leaves == 0 ? 1 : num_leaves), imm_->ApproximateMemoryUsage(), segment_manager_->ApproximateSize());

        return s;
    }
}

static int num_compactions = 0;

Status SilkStore::DoCompactionWork(WriteBatch &leaf_index_wb, NvmemTable* imm) {
    mutex_.Unlock();
    
    ReadOptions ro;
    ro.snapshot = leaf_index_->GetSnapshot();

    // Release snapshot after the traversal is done
    DeferCode c([&ro, this]() {
        leaf_index_->ReleaseSnapshot(ro.snapshot);
        mutex_.Lock();
    });

    std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));
    int self_compaction = 0;
    int num_leaves_snap = (num_leaves == 0 ? 1 : num_leaves);
    int num_splits = 0;
    iit->SeekToFirst();
    std::unique_ptr<Iterator> mit(imm->NewIterator());
    mit->SeekToFirst();
    std::string buf, buf2;
    uint32_t run_no;
    Status s;

    GroupedSegmentAppender grouped_segment_appender(1, segment_manager_, options_);
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
        // Record the data read from leaf_index as well
        stats_.Add(iit->key().size() + iit->value().size(), 0);
        SegmentBuilder *seg_builder = nullptr;
        bool switched_segment = false;
        s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder, switched_segment);
        if (!s.ok())
            return s;
        if (switched_segment && leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
            // If all previous segments are built successfully and
            // the leaf_index write buffer exceeds the threshold,
            // write it down to leaf_index_ to keep the memory footprint small.
            s = leaf_index_->Write({}, &leaf_index_wb);
            if (!s.ok())
                return s;
            leaf_index_wb.Clear();
        }

        uint32_t seg_id = seg_builder->SegmentId();
        assert(seg_builder->RunStarted() == false);
        int minirun_key_cnt = 0;
        // Build up a minirun of key value payloads
        while (mit->Valid()) {
            Slice imm_internal_key = mit->key();
            ParsedInternalKey parsed_internal_key;
         //   std::cout << "debug parsed_internal_key " << imm_internal_key.ToString() << " ";

            if (!ParseInternalKey(imm_internal_key, &parsed_internal_key)) {
                s = Status::InvalidArgument("error parsing key from immutable table 1 during compaction");
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

            // Reading data from memtable costs no read io.
            // Record the write to segment.
            stats_.Add(0, mit->key().size() + mit->value().size());
            ++minirun_key_cnt;

            mit->Next();
        }
        stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);

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
            LeafIndexEntryBuilder::AppendMiniRunIndexEntry(leaf_index_entry, new_minirun_index_entry, &buf2,
                                                           &new_leaf_index_entry);

            assert(leaf_index_entry.GetNumMiniRuns() + 1 == new_leaf_index_entry.GetNumMiniRuns());
            // Write out the updated entry to leaf index
            leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
            stat_store_.UpdateLeafNumRuns(leaf_max_key.ToString(), new_leaf_index_entry.GetNumMiniRuns());
        } else {
            // Memtable has no keys intersected with this leaf
            if (leaf_index_entry.Empty()) {
                // If the leaf became empty due to self-compaction or split, remove it from the leaf index
                leaf_index_wb.Delete(leaf_max_key);
                --num_leaves;
                stat_store_.DeleteLeaf(leaf_max_key.ToString());
                //fprintf(stderr, "Deleted index entry for empty leaf of key %s\n", leaf_max_key.ToString().c_str());
            }
        }

        iit->Next();
        if (iit->Valid()) {
            next_leaf_max_key = iit->key();
            next_leaf_index_value = iit->value();
        }
    }
    // Memtable has keys that are greater than all the keys in leaf_index_.
    // In this case, partition the rest of memtable contents into leaves each no more than options_.leaf_datasize_thresh bytes in size.
    while (s.ok() && mit->Valid()) {
        std::string buf, buf2;
        SegmentBuilder *seg_builder = nullptr;
        bool switched_segment = false;
        s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder, switched_segment);
        if (!s.ok())
            return s;
        if (switched_segment && leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
            // If all previous segments are built successfully and
            // the leaf_index write buffer exceeds the threshold,
            // write it down to leaf_index_ to keep the memory footprint small.
            s = leaf_index_->Write({}, &leaf_index_wb);
            if (!s.ok())
                return s;
            leaf_index_wb.Clear();
        }

        uint32_t seg_id = seg_builder->SegmentId();

        assert(seg_builder->RunStarted() == false);
        s = seg_builder->StartMiniRun();
        if (!s.ok()) {
            fprintf(stderr,"%s", s.ToString().c_str());
            return s;
        }
        size_t bytes = 0;
        int minirun_key_cnt = 0;
        while (mit->Valid()) {
            
           // imm_->print();

            Slice imm_internal_key = mit->key();
            ParsedInternalKey parsed_internal_key;
           // std::cout << "debug parsed_internal_key " << imm_internal_key.ToString() << " ";
            if (!ParseInternalKey(mit->key(), &parsed_internal_key)) { 
                s = Status::InvalidArgument("error parsing key from immutable table 2 during compaction");
                fprintf(stderr,"%s", s.ToString().c_str());
                return s;
            }
            // A leaf holds at least one key-value pair and at most options_.leaf_datasize_thresh bytes of data.
            if (minirun_key_cnt > 0 &&
                bytes + imm_internal_key.size() + mit->value().size() >= options_.leaf_datasize_thresh * 0.95) {
                break;
            }
            bytes += imm_internal_key.size() + mit->value().size();
            leaf_max_key = parsed_internal_key.user_key;

            seg_builder->Add(imm_internal_key, mit->value());
            // Reading data from memtable costs no read io.
            // Record the write to segment.
            stats_.Add(0, mit->key().size() + mit->value().size());
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
        leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
        ++num_leaves;
        stat_store_.NewLeaf(leaf_max_key.ToString());
        stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);
    }
    ++num_compactions;
    return s;
}

// Perform a merge between leaves and the immutable memtable.
// Single threaded version.
void SilkStore::BackgroundCompaction() {


    /* if (imm_.size() > 2){
        NvmemTable * tmp = imm_.front();
        int size = imm_.size();
        for(int i = 1; i < size; i++){
            //std::unique_ptr<a> mit(imm_[i]->NewIterator());
            IndexIterator mit = imm_[i]->NewIndexIterator();
            while (mit.valid()){
                tmp->AddIndex(mit.key(), mit.value());
                ++mit;
            }
            NvmemTable *removeImm = imm_[i];
            imm_.erase(imm_.begin() + 1);
            removeImm->Unref();
        }
    }
 */

 

 while(!imm_.empty()){
    std::cout << "BackgroundCompaction imm's nums: " << imm_.size() << "\n";
    auto it = imm_.front();
    auto t_start_compaction = env_->NowMicros();
    DeferCode c([this, t_start_compaction]() { stats_.AddTimeCompaction(env_->NowMicros() - t_start_compaction); });
    mutex_.Unlock();
    Status s;
    bool full_compacted = false;
    while (options_.maximum_segments_storage_size && segment_manager_->ApproximateSize() >=
                                                     options_.segments_storage_size_gc_threshold *
                                                     options_.maximum_segments_storage_size && s.ok()) {
        auto t_start_gc = env_->NowMicros();
        if (this->GarbageCollect() == 0) {
            // Do a full compaction to release space
            fprintf(stderr, "full compaction\n");
            mutex_.Lock();
            s = MakeRoomInLeafLayer(true);
            mutex_.Unlock();
            full_compacted = true;
        }
        stats_.AddTimeGC(env_->NowMicros() - t_start_gc);
    }
    mutex_.Lock();
    if (!s.ok()) {
        bg_error_ = s;
        return;
    }
    if (full_compacted == false) {
        s = MakeRoomInLeafLayer();
        if (!s.ok()) {
            bg_error_ = s;
            return;
        }
    }
    WriteBatch leaf_index_wb;
    s = DoCompactionWork(leaf_index_wb,it);
    if (!s.ok()) {
        Log(options_.info_log, "DoCompactionWork failed: %s\n", s.ToString().c_str());
        bg_error_ = s;
    } else {
        mutex_.Unlock();
        if (leaf_index_wb.ApproximateSize()) {
            s = leaf_index_->Write({}, &leaf_index_wb);
        }
        mutex_.Lock();
        if (!s.ok()) {
            bg_error_ = s;
            Log(options_.info_log, "DoCompactionWork failed: %s\n", s.ToString().c_str());
            return;
        }
        // Save a new Current File
        SetCurrentFileWithLogNumber(env_, dbname_, logfile_number_);
        // Commit to the new state
        nvm_manager_->free();
       // std::cout << "imm_ compaction finished !\n";
        it->Unref();
        imm_.pop_front();
        has_imm_.Release_Store(nullptr);
    }
    }
}

Status DestroyDB(const std::string &dbname, const Options &options) {
    Status result = leveldb::DestroyDB("/mnt/myPMem/leaf_index", options);
    if (result.ok() == false)
        return result;
    Env *env = options.env;
    std::vector<std::string> filenames;
    result = env->GetChildren(dbname, &filenames);
    if (!result.ok()) {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    FileLock *lock;
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