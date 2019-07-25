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
#include "silkstore/silkstore_iter.h"
#include "silkstore/util.h"


namespace leveldb {

Status DB::OpenSilkStore(const Options &options,
                         const std::string &name,
                         DB **dbptr) {
    *dbptr = nullptr;
    silkstore::SilkStore* store = new silkstore::SilkStore(options, name);
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

static void SilkStoreNewIteratorCleanup(void * arg1, void *arg2) {
    static_cast<MemTable*>(arg1)->Unref();
    if (arg2) static_cast<MemTable*>(arg2)->Unref();
}

Iterator* SilkStore::NewIterator(const ReadOptions& ropts) {
    MutexLock l(&mutex_);
    const SnapshotImpl * snapshot = dynamic_cast<const SnapshotImpl*>(leaf_index_->GetSnapshot());
    // Collect together all needed child iterators
    std::vector<Iterator*> list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
        imm_->Ref();
    }
    list.push_back(leaf_store_->NewIterator(ropts));
    Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
    internal_iter->RegisterCleanup(SilkStoreNewIteratorCleanup, mem_, imm_);
    return leveldb::silkstore::NewDBIterator(internal_comparator_.user_comparator(), internal_iter, snapshot->sequence_number());
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

std::pair<uint32_t, uint32_t> SilkStore::ChooseLeafCompactionRunRange(const LeafIndexEntry & leaf_index_entry) {
    // TODO: come up with a better approach
    uint32_t num_runs = leaf_index_entry.GetNumMiniRuns();
    assert(num_runs > 1);
    return {num_runs - 2, num_runs - 1};
}

Status
SilkStore::SplitLeaf(SegmentBuilder * seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, std::string * l1_max_key_buf, std::string * l2_max_key_buf, std::string * l1_index_entry_buf, std::string * l2_index_entry_buf) {
    // TODO: implement leaf split
}

LeafIndexEntry SilkStore::CompactLeaf(SegmentBuilder * seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s, std::string * buf, uint32_t start_minirun_no, uint32_t end_minirun_no) {
    Iterator *it = leaf_store_->NewIteratorForLeaf(ReadOptions{}, leaf_index_entry, s, start_minirun_no, end_minirun_no);
    if (!s.ok()) return {};
    DeferCode c([it](){delete it;});

    s = seg_builder->StartMiniRun();
    it->SeekToFirst();
    std::string current_user_key;
    bool has_current_user_key = false;
    while (it->Valid()) {
        Slice key = it->key();
        ParsedInternalKey ikey;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
        } else {
            if (!has_current_user_key ||
                user_comparator()->Compare(ikey.user_key,
                                           Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                seg_builder->Add(it->key(), it->value());
            }
        }
        it->Next();
    }
    uint32_t run_no;
    seg_builder->FinishMiniRun(&run_no);
    std::string buf2;
    MiniRunIndexEntry replacement = MiniRunIndexEntry::Build(seg_no, run_no,
                                                             seg_builder->GetFinishedRunIndexBlock(),
                                                             seg_builder->GetFinishedRunFilterBlock(),
                                                             &buf2);
    LeafIndexEntry new_leaf_index_entry;
    s = LeafIndexEntryBuilder::ReplaceMiniRunRange(leaf_index_entry, start_minirun_no, end_minirun_no, replacement,
                                                   buf, &new_leaf_index_entry);
    if (!s.ok()) return {};
    return new_leaf_index_entry;
}

// Perform a merge  between leaves and the immutable memtable.
// Single threaded version.
void SilkStore::BackgroundCompaction() {
    ReadOptions ro;
    ro.snapshot = leaf_index_->GetSnapshot();
    Iterator * iit = leaf_index_->NewIterator(ro);
    iit->SeekToFirst();
    Iterator * mit = imm_->NewIterator();
    mit->SeekToFirst();
    DeferCode c([iit, mit](){delete iit; delete mit;});

    std::string buf, buf2, buf3, buf4, buf5, buf6;
    uint32_t run_no;
    SegmentBuilder * seg_builder;
    uint32_t seg_id;

    Status s = segment_manager_->NewSegmentBuilder(&seg_id, &seg_builder);
    if (!s.ok()) {
        bg_error_ = s;
        return;
    }
    Slice next_leaf_max_key;
    Slice next_leaf_index_value;
    Slice leaf_max_key;
    LeafIndexEntry leaf_index_entry;
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
        if (next_leaf_max_key.empty()) {
            next_leaf_max_key = iit->key();
            next_leaf_index_value = iit->value();
        }

        Slice leaf_max_key = next_leaf_max_key;
        LeafIndexEntry leaf_index_entry(next_leaf_index_value);
        enum {
            leaf_compacted,
            leaf_splitted,
            leaf_intact
        };
        int leaf_state = leaf_intact;
        if (leaf_index_entry.GetNumMiniRuns() >= options_.leaf_max_num_miniruns) {
            // TODO: perform leaf split compaction or in-leaf compaction
            // TODO: come up with better appraoch of choosing leaf split or in-leaf compaction
            if (rand() % 2 == 0) {
                buf3.clear();
                std::pair<uint32_t, uint32_t> p = ChooseLeafCompactionRunRange(leaf_index_entry);
                uint32_t start_minirun_no = p.first;
                uint32_t end_minirun_no = p.second;
                LeafIndexEntry new_leaf_index_entry = CompactLeaf(seg_builder, seg_id, leaf_index_entry, s, &buf3, start_minirun_no, end_minirun_no);
                if (!s.ok()) {
                    bg_error_ = s;
                    return;
                }
                // Invalidate compacted runs
                for (uint32_t i = start_minirun_no; i <= end_minirun_no; ++i) {
                    s = segment_manager_->InvalidateSegmentRun(seg_id, run_no);
                    if (!s.ok()) {
                        bg_error_ = s;
                        return;
                    }
                }
                leaf_index_entry = new_leaf_index_entry;
                leaf_state = leaf_compacted;
            } else {
                s = SplitLeaf(seg_builder, seg_id, leaf_index_entry, &buf3, &buf4, &buf5, &buf6);
                if (!s.ok()) {
                    bg_error_ = s;
                    return;
                }
                leaf_max_key = buf3;
                leaf_index_entry = LeafIndexEntry(buf5);
                leaf_state = leaf_splitted;
            }
        }

        // Build up a minirun of key value payloads
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
            if (seg_builder->RunStarted() == false) {
                s = seg_builder->StartMiniRun();
                if (!s.ok()) {
                    bg_error_ = s;
                    return;
                }
                assert(seg_builder->RunStarted());
            }
            seg_builder->Add(imm_internal_key, mit->value());
            mit->Next();
        }

        if (seg_builder->RunStarted()) {
            s = seg_builder->FinishMiniRun(&run_no);
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }

            // Generate an index entry for the new minirun
            buf.clear();
            MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                                                                 seg_builder->GetFinishedRunIndexBlock(),
                                                                                 seg_builder->GetFinishedRunFilterBlock(),
                                                                                 &buf);

            // Update the leaf index entry
            LeafIndexEntry new_leaf_index_entry;
            buf2.clear();
            s = LeafIndexEntryBuilder::AppendMiniRunIndexEntry(leaf_index_entry, new_minirun_index_entry, &buf2, &new_leaf_index_entry);
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }

            // Write out the updated entry to leaf index
            s = leaf_index_->Put(WriteOptions{}, leaf_max_key, new_leaf_index_entry.GetRawData());
            if (!s.ok()) {
                bg_error_ = s;
                return;
            }
        } else {
            if (leaf_state == leaf_splitted) {
                // Leaf split into two leaves and the memtable has no keys intersect with the first leaf
                // In this case, we update the leaf index entry only
                s = leaf_index_->Put(WriteOptions{}, leaf_max_key, leaf_index_entry.GetRawData());
                if (!s.ok()) {
                    bg_error_ = s;
                    return;
                }
            }
        }

        if (leaf_state == leaf_intact || leaf_state == leaf_compacted) {
            iit->Next();
            if (iit->Valid()) {
                next_leaf_max_key = iit->key();
                next_leaf_index_value = iit->value();
            }
        } else { // leaf_splitted
            next_leaf_max_key = buf3;
            next_leaf_index_value = buf6;
        }
    }

    if (s.ok() && mit->Valid()) {
        // Memtable has keys that are greater than all the keys in leaf_index
        // TODO: In this case, create new leaves whose runs store no more than 1MB of data each.
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
            leaf_max_key = parsed_internal_key.user_key;
            seg_builder->Add(imm_internal_key, mit->value());
            mit->Next();
        }
        uint32_t run_no;
        seg_builder->FinishMiniRun(&run_no);

        // Generate an index entry for the new minirun
        buf.clear();
        MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(seg_id, run_no,
                                                                         seg_builder->GetFinishedRunIndexBlock(),
                                                                         seg_builder->GetFinishedRunFilterBlock(),
                                                                         &buf);
        LeafIndexEntry new_leaf_index_entry;
        buf2.clear();
        s = LeafIndexEntryBuilder::AppendMiniRunIndexEntry(LeafIndexEntry{}, minirun_index_entry, &buf2, &new_leaf_index_entry);
        if (!s.ok()) {
            bg_error_ = s;
            return;
        }

        s = leaf_index_->Put(WriteOptions{}, leaf_max_key, new_leaf_index_entry.GetRawData());
        if (!s.ok()) {
            bg_error_ = s;
            return;
        }
    }
    s = seg_builder->Finish();
    if (!s.ok()) {
        bg_error_ = s;
    }
}

}  // namespace silkstore
}  // namespace leveldb