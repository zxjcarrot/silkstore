//
// Created by zxjcarrot on 2019-07-05.
//

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/write_batch.h"
#include "table/merger.h"
#include "util/mutexlock.h"

#include "silkstore/silkstore_impl.h"
#include "silkstore/util.h"

namespace leveldb {
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
          manual_compaction_(nullptr) {
    has_imm_.Release_Store(nullptr);
}

Status SilkStore::OpenIndex(const Options &index_options) {
    assert(leaf_index_ == nullptr);
    return DB::Open(index_options, dbname_ + "/leaf_index", &leaf_index_);
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
    Status s = OpenIndex(this->options_);

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
        return env_->RenameFile(temp_current, CurrentFilename(dbname_));
    } else {
        SequenceNumber log_start_seq_num = std::stoi(current_content);
        return RecoverLogFile(log_start_seq_num, &max_sequence_);
    }
}


// Convenience methods
Status SilkStore::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
    return Status::NotSupported("Not Implemented");
}

Status SilkStore::Delete(const WriteOptions& options, const Slice& key) {
    return Status::NotSupported("Not Implemented");
}

Iterator* SilkStore::NewIterator(const ReadOptions& ropts) {
    return nullptr;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status SilkStore::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    bool allow_delay = !force;
    Status s;
    while (true) {
        if (!force && (mem_->ApproximateMemoryUsage() <= memtable_capacity_)) {
            break;
        } else if (imm_ != nullptr) {
            Log(options_.info_log, "Current memtable full;Compaction ongoing; waiting...\n");
            background_work_finished_signal_.Wait();
        } else {
            // Attempt to switch to a new memtable and trigger compaction of old
            uint64_t new_log_number = max_sequence_;
            WritableFile* lfile = nullptr;
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
            size_t new_memtable_capacity = (memtable_capacity_ + segment_manager_->ApproximateSize()) / options_.memtbl_to_L0_ratio;
            new_memtable_capacity = std::min(options_.max_memtbl_capacity, std::max(options_.write_buffer_size, new_memtable_capacity));
            memtable_capacity_ = new_memtable_capacity;
            mem_ = new MemTable(internal_comparator_);
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

void SilkStore::BGWork(void* db) {
    reinterpret_cast<SilkStore*>(db)->BackgroundCall();
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
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex* mu) : cv(mu) { }
};


Status SilkStore::Write(const WriteOptions& options, WriteBatch* my_batch) {
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
    Writer* last_writer = &w;
    if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
        WriteBatch* updates = BuildBatchGroup(&last_writer);
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
        Writer* ready = writers_.front();
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

Status SilkStore::Get(const ReadOptions &options,
           const Slice &key,
           std::string *value) {

    Status s;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;
    if (options.snapshot != nullptr) {
        snapshot =
                static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
    } else {
        snapshot = max_sequence_;
    }

    MemTable* mem = mem_;
    MemTable* imm = imm_;
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
            s = leaf_store_->Get(options, lkey, value);
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
WriteBatch* SilkStore::BuildBatchGroup(Writer** last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    size_t max_size = 1 << 20;
    if (size <= (128<<10)) {
        max_size = size + (128<<10);
    }

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer* w = *iter;
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

// Perform a merge  between leaves and the immutable memtable.
// Single threaded version.
void SilkStore::BackgroundCompaction() {
    ReadOptions ro;
    Iterator * iit = leaf_index_->NewIterator(ro);
    iit->SeekToFirst();
    Iterator * mit = imm_->NewIterator();
    mit->SeekToFirst();
    DeferCode c([iit, mit](){delete iit; delete mit;});

    std::string buf, buf2;
    uint32_t run_no;
    SegmentBuilder * seg_builder;
    uint32_t seg_id;

    Status s = segment_manager_->NewSegmentBuilder(&seg_id, &seg_builder);
    if (!s.ok()) {
        bg_error_ = s;
        return;
    }

    while (iit->Valid() && mit->Valid() && s.ok()) {
        if (seg_builder->FileSize() > options_.segment_file_size_thresh) {
            s = seg_builder->Finish();
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }
            delete seg_builder;
            s = segment_manager_->NewSegmentBuilder(&seg_id, &seg_builder);
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }
        }

        Slice leaf_max_key = iit->key();
        LeafIndexEntry leaf_index_entry(iit->value());
        if (leaf_index_entry.GetNumMiniRuns() >= options_.leaf_max_num_miniruns) {
            // TODO: perform leaf split or in-leaf compaction

        } else {
            // Build up a minirun of key value payloads
            s = seg_builder->StartMiniRun();
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }
            while(mit->Valid()) {
                Slice imm_internal_key = mit->key();
                ParsedInternalKey parsed_internal_key;
                if (!ParseInternalKey(mit->key(), &parsed_internal_key)) {
                    bg_error_ = Status::InvalidArgument("error parsing key from immutable table during compaction");
                    return;
                }
                if (options_.comparator->Compare(parsed_internal_key.user_key, leaf_max_key) > 0) {
                    break;
                }
                seg_builder->Add(imm_internal_key, mit->value());
                mit->Next();
            }
            s = seg_builder->FinishMiniRun(&run_no);
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }

            // Generate an index entry for the new minirun
            buf.clear();
            MiniRunIndexEntry::EncodeMiniRunIndexEntry(seg_id, run_no, seg_builder->GetFinishedRunIndexBlock(), seg_builder->GetFinishedRunFilterBlock(), &buf);
            MiniRunIndexEntry new_minirun_index_entry(buf);

            // Update the leaf index entry
            LeafIndexEntry new_leaf_index_entry;
            buf2.clear();
            s = LeafIndexEntryBuilder::AppendMiniRunIndexEntry(leaf_index_entry, new_minirun_index_entry, &buf2, &new_leaf_index_entry);
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }

            // Write out the updated entry to leaf index
            WriteOptions wo;
            s = leaf_index_->Put(wo, leaf_max_key, new_leaf_index_entry.GetRawData());
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }
        }

        iit->Next();
    }
}

}  // namespace silkstore
}  // namespace leveldb