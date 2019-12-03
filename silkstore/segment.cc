//
// Created by zxjcarrot on 2019-06-29.
//

#include "silkstore/segment.h"

#include <cmath>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <queue>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

static std::string MakeSegmentFileName(uint32_t segment_id) {
    return "seg." + std::to_string(segment_id);
}

struct Segment::Rep {
    /*
     * Stores the ids of the minirun that have been invalidated.
     * During GC, runs indicated by this set are skipped querying leaf index
     * and directly considered as garbage.
     */
    std::string invalidated_runs;
    std::vector<MiniRunHandle> run_handles;
    uint32_t id;
    RandomAccessFile *file;
    uint64_t file_size;
    Options options;
    std::atomic<int> ref_cnt;

    Rep() : ref_cnt(0) {}
};

Status Segment::InvalidateMiniRun(const int &run_no) {
    Rep *r = rep_;
    PutVarint32(&r->invalidated_runs, run_no);
    return Status::OK();
}

uint32_t Segment::SegmentId() const {
    Rep *r = rep_;
    return r->id;
}

size_t Segment::SegmentSize() const {
    Rep *r = rep_;
    return r->file_size;
}

Segment::~Segment() {
    delete rep_;
    rep_ = nullptr;
}

Status Segment::Open(const Options &options, uint32_t segment_id,
                     RandomAccessFile *file, uint64_t file_size,
                     silkstore::Segment **segment) {
    Rep *r = new Rep;
    r->file = file;
    r->file_size = file_size;
    r->id = segment_id;
    r->options = options;
    *segment = new Segment(r);

    size_t footer_offset = file_size - sizeof(uint64_t);
    char footer_buf[sizeof(uint64_t)];
    Slice footer_input;
    Status s = file->Read(footer_offset, sizeof(uint64_t), &footer_input, footer_buf);
    if (!s.ok()) return s;

    uint64_t minirun_index_block_size = DecodeFixed64(footer_input.data());
    char minirun_index_buf[minirun_index_block_size];
    Slice minirun_index_input;
    s = file->Read(footer_offset - minirun_index_block_size, minirun_index_block_size, &minirun_index_input,
                   minirun_index_buf);
    if (!s.ok()) return s;

    r->run_handles.reserve(minirun_index_block_size / sizeof(MiniRunHandle));
    const char *minirun_index_buf_end = minirun_index_input.data() + minirun_index_block_size;
    for (auto p = minirun_index_input.data(); p < minirun_index_buf_end; p += sizeof(MiniRunHandle)) {
        uint64_t run_starting_pos = DecodeFixed64(p);
        uint64_t last_block_offset = DecodeFixed64(p + sizeof(uint64_t));
        uint64_t last_block_size = DecodeFixed64(p + sizeof(uint64_t) * 2);
        BlockHandle last_block_handle;
        last_block_handle.set_offset(last_block_offset);
        last_block_handle.set_size(last_block_size);
        r->run_handles.emplace_back(MiniRunHandle{run_starting_pos, last_block_handle});
    }
    return Status::OK();
}

RandomAccessFile* Segment::SetNewSegmentFile(RandomAccessFile* file) {
    Rep *r = rep_;
    assert(NumRef() == 0);
    auto old_file = r->file;
    r->file = file;
    return old_file;
}

Status Segment::OpenMiniRun(int run_no, Block & index_block, MiniRun ** run) {
    Rep *r = rep_;
    if (run_no < 0 || run_no >= r->run_handles.size())
        return Status::InvalidArgument("run_no is not in valid range");
    uint64_t run_offset = r->run_handles[run_no].run_start_pos;
    uint64_t run_size =
            run_no + 1 == r->run_handles.size() ? r->file_size - run_offset : r->run_handles[run_no + 1].run_start_pos - run_offset;

    *run = new MiniRun(&r->options, r->file, run_offset, run_size, index_block);
    return Status::OK();
}

void Segment::Ref() {
    Rep *r = rep_;
    r->ref_cnt.fetch_add(1);
}

void Segment::UnRef() {
    Rep *r = rep_;
    r->ref_cnt.fetch_add(-1);
}

int Segment::NumRef() {
    Rep *r = rep_;
    return r->ref_cnt.load();
}

void Segment::ForEachRun(std::function<bool(int, MiniRunHandle, size_t, bool)> processor) {
    std::unordered_set<int> invalidated_runs;
    Rep *r = rep_;
    const char *start = r->invalidated_runs.data();
    const char *end = start + r->invalidated_runs.size();
    while (start < end) {
        uint32_t run_no;
        uint32_t run_size;
        start = GetVarint32Ptr(start, end, &run_no);
        invalidated_runs.insert(run_no);
    }

    for (size_t run_no = 0; run_no < r->run_handles.size(); ++run_no) {
        bool valid = invalidated_runs.find(run_no) == invalidated_runs.end();
        size_t run_size = run_no == r->run_handles.size() - 1 ? r->file_size - r->run_handles[run_no].run_start_pos:
                          r->run_handles[run_no + 1].run_start_pos - r->run_handles[run_no].run_start_pos;
        bool early_exit = processor(run_no, r->run_handles[run_no], run_size, valid);
        if (early_exit)
            break;
    }
}

struct SegmentManager::Rep {
    std::mutex mutex;
    std::unordered_map<uint32_t, Segment*> segments;
    std::unordered_map<uint32_t, std::string> segment_filepaths;
    uint32_t seg_id_max = 0;
    Options options;
    std::string dbname;
    std::function<void()> gc_func;
};

static bool GetSegmentFileInfo(const std::string & filename, uint32_t & seg_id) {
    if (filename.find("seg.") == 0) {
        std::string seg_id_str(filename.begin() + 4, filename.end());
        seg_id = std::stoi(seg_id_str);
        return true;
    }
    return false;
}


void SegmentManager::ForEachSegment(std::function<void(Segment* seg)> processor) {
    Rep *r = rep_;
    std::unordered_set<uint32_t> segment_ids;
    {
        std::lock_guard<std::mutex> g(r->mutex);
        for (auto kv : r->segment_filepaths) {

            if (kv.second.find("tmpseg.") == std::string::npos) {
                segment_ids.insert(kv.first);
            }
        }
    }
    for (auto segment_id : segment_ids) {
        Segment * seg;
        Status s = OpenSegment(segment_id, &seg);
        if (s.ok()) {
            processor(seg);
            seg->UnRef();
        }
    }
}

std::vector<Segment *> SegmentManager::GetMostInvalidatedSegments(int K) {
    struct QueueNode {
        Segment *seg;
        double invalidated_data_ratio;

        bool operator<(const QueueNode &rhs) const {
            return invalidated_data_ratio > rhs.invalidated_data_ratio;
        }
    };
    std::priority_queue<QueueNode> q;


    ForEachSegment([&, this](Segment *seg){
        uint64_t invalidated_data_size = 0;
        seg->ForEachRun([&invalidated_data_size](int, MiniRunHandle, size_t size, bool valid) {
            if (valid == false) {
                invalidated_data_size += size;
            }
            return false;
        });
        double invalidated_data_ratio = invalidated_data_size / (seg->SegmentSize());
        if (abs(invalidated_data_ratio - 0) < 1e-7)
            return false;
        if (q.size() < K) {
            q.push({seg, invalidated_data_ratio});
        } else {
            if (q.empty() == false && q.top().invalidated_data_ratio < invalidated_data_ratio) {
                q.pop();
                q.push({seg, invalidated_data_ratio});
            }
        }
        return false;
    });

    std::vector<Segment *> res;
    while (!q.empty()) {
        res.push_back(q.top().seg);
        q.pop();
    }
    return res;
}


Status SegmentManager::NewSegmentBuilder(uint32_t *seg_id, std::unique_ptr<SegmentBuilder> &seg_builder_ptr, bool gc_on_segment_shortage) {
    Rep*r = rep_;
    Env* default_env = Env::Default();

//    while (gc_on_segment_shortage && r->options.maximum_segments_storage_size && ApproximateSize() >= r->options.segments_storage_size_gc_threshold * r->options.maximum_segments_storage_size) {
////        // TODO: initiate garbage collection
////        r->gc_func();
////    }

    std::lock_guard<std::mutex> g(r->mutex);
    uint32_t exp_seg_id = r->seg_id_max + 1;
    std::string src_segment_filepath = r->dbname + "/tmpseg." + std::to_string(exp_seg_id);
    std::string target_segment_filepath = r->dbname + "/seg." + std::to_string(exp_seg_id);
    WritableFile* wfile = nullptr;
    Status s = default_env->NewAppendableFile(src_segment_filepath, &wfile);
    if (!s.ok()) {
        return s;
    }
    seg_builder_ptr.reset(new SegmentBuilder(r->options, src_segment_filepath, target_segment_filepath, wfile, exp_seg_id, this));
    r->seg_id_max = *seg_id = exp_seg_id;
    r->segment_filepaths[*seg_id] = src_segment_filepath;
    return Status::OK();
}

Status SegmentManager::InvalidateSegmentRun(uint32_t seg_id, uint32_t run_no) {
    Segment * seg;
    Status s = OpenSegment(seg_id, &seg);
    if (!s.ok()) return s;
    Rep* r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    s = seg->InvalidateMiniRun(run_no);
    DropSegment(seg);
    return s;
}



Status SegmentManager::RenameSegment(uint32_t seg_id, const std::string target_filepath) {
    Rep *r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    auto filepath_it = r->segment_filepaths.find(seg_id);
    if (filepath_it == r->segment_filepaths.end())
        return Status::NotFound("RenameSegment failed because the segment does not exist");
    const std::string filepath = filepath_it->second;
    r->segment_filepaths[seg_id] = target_filepath;
    Status s = Env::Default()->RenameFile(filepath, target_filepath);
    if (!s.ok())
        return s;
    auto segment_it = r->segments.find(seg_id);
    if (segment_it == r->segments.end())
        return s;
    Segment *segment = segment_it->second;
    // Wait for all the old readers to drop reference to the current segment
    // New readers will be blocked by r->mutex
    while (segment->NumRef())
        Env::Default()->SleepForMicroseconds(10);

    RandomAccessFile *rfile;
    s = Env::Default()->NewRandomAccessFile(target_filepath, &rfile);
    if (!s.ok()) {
        return s;
    }
    RandomAccessFile * old_file = segment->SetNewSegmentFile(rfile);
    //FIXME: fix this leak
    //delete old_file;
    return Status::OK();
}

size_t SegmentManager::ApproximateSize() {
    Rep* r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    size_t size = 0;
    Env* default_env = Env::Default();
    for (auto kv : r->segment_filepaths) {
        auto filepath = kv.second;
        uint64_t filesize;
        Status s = default_env->GetFileSize(filepath, &filesize);
        if (s.ok()) {
            size += filesize;
        } else {
            Log(r->options.info_log, "Failed getting size of file %s\n", filepath.c_str());
        }
    }
    //Log(r->options.info_log, "Approximate size of all segment files %lu\n", size);
    return size;
}

Status SegmentManager::RemoveSegment(uint32_t seg_id) {
    Status s;
    Rep* r = rep_;
    r->mutex.lock();
    auto filepath_it = r->segment_filepaths.find(seg_id);
    if (filepath_it == r->segment_filepaths.end()) {
        r->mutex.unlock();
        return Status::NotFound("segment[" + std::to_string(seg_id) + "] is not found");
    }

    auto it = r->segments.find(seg_id);
    if (it != r->segments.end()) {
        std::string filepath = filepath_it->second;
        Segment* seg = it->second;
        r->segments.erase(seg_id);
        r->segment_filepaths.erase(seg_id);
        Env* default_env = Env::Default();
        r->mutex.unlock();
        // Wait for all read references to this segment to drop
        while (seg->NumRef())
            default_env->SleepForMicroseconds(10);
        s = default_env->DeleteFile(filepath);
        if (!s.ok())
            return s;
        r->mutex.lock();
        delete seg;
    }
    r->mutex.unlock();
    return s;
}

void SegmentManager::DropSegment(Segment *seg_ptr) {
    seg_ptr->UnRef();
}

Status SegmentManager::OpenSegment(uint32_t seg_id, Segment **seg_ptr) {
    Rep* r = rep_;
    r->mutex.lock();
    auto filepath_it = r->segment_filepaths.find(seg_id);
    if (filepath_it == r->segment_filepaths.end()) {
        r->mutex.unlock();
        return Status::NotFound("segment[" + std::to_string(seg_id) + "] is not found");
    }

    auto it = r->segments.find(seg_id);
    if (it == r->segments.end()) {
        std::string filepath = filepath_it->second;
        r->mutex.unlock();
        Env* default_env = Env::Default();
        RandomAccessFile*rfile;
        Status s = default_env->NewRandomAccessFile(filepath, &rfile);
        if (!s.ok()) {
            return s;
        }

        uint64_t filesize;
        s = default_env->GetFileSize(filepath, &filesize);
        if (!s.ok()) {
            return s;
        }
        r->mutex.lock();
        s = Segment::Open(r->options, seg_id, rfile, filesize, seg_ptr);
        if (!s.ok()) {
            r->mutex.unlock();
            return s;
        }
        r->segments[seg_id] = *seg_ptr;
        r->segment_filepaths[seg_id] = filepath;
    } else {
        *seg_ptr = it->second;
    }
    (*seg_ptr)->Ref();
    r->mutex.unlock();
    return Status::OK();
}

Status SegmentManager::OpenManager(const Options& options,
                                   const std::string& dbname,
                                   SegmentManager** manager_ptr,
                                   std::function<void()> gc_func) {
    Env* default_env = Env::Default();
    if (default_env->FileExists(dbname) == false) {
        if (options.create_if_missing == false) {
            return Status::NotFound("dbname[" + dbname + "] not found");
        }
        Status s = default_env->CreateDir(dbname);
        if (!s.ok()) {
            return s;
        }
    }
    Rep* r = new Rep;
    r->options = options;
    r->dbname = dbname;
    r->gc_func = gc_func;
    std::vector<std::string> subfiles;
    Status s = default_env->GetChildren(dbname, &subfiles);
    if (!s.ok()) {
        return s;
    }

    std::vector<std::string> seg_files;
    std::vector<uint32_t> seg_ids;
    for (auto filename : subfiles) {
        uint32_t seg_id = -1;
        if (GetSegmentFileInfo(filename, seg_id)) {
            std::string filepath = dbname + "/" + filename;
            seg_files.push_back(filepath);
            seg_ids.push_back(seg_id);
            r->segment_filepaths[seg_id] = filepath;
            r->seg_id_max = std::max(r->seg_id_max, seg_id);
        }
    }

    *manager_ptr = new SegmentManager(r);
    return Status::OK();
}

}  // namespace silkstore
}  // namespace leveldb