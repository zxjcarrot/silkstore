//
// Created by zxjcarrot on 2019-06-29.
//
#include <cmath>
#include <string>
#include <unordered_set>
#include <unordered_map>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include "silkstore/minirun.h"
#include "silkstore/segment.h"


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
};

Status Segment::InvalidateMiniRun(const int &run_no) {
    Rep *r = rep_;
    leveldb::PutVarint32(&r->invalidated_runs, run_no);
}

Status Segment::Open(const Options &options, uint32_t segment_id, RandomAccessFile *file, uint64_t file_size,
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

    uint64_t minirun_index_block_size = leveldb::DecodeFixed64(footer_buf);
    char minirun_index_buf[minirun_index_block_size];
    Slice minirun_index_input;
    s = file->Read(footer_offset - minirun_index_block_size, minirun_index_block_size, &minirun_index_input,
                   minirun_index_buf);
    if (!s.ok()) return s;

    r->run_handles.reserve(minirun_index_block_size / sizeof(uint64_t));
    const char *minirun_index_buf_end = minirun_index_buf + minirun_index_block_size;
    for (auto p = minirun_index_buf; p < minirun_index_buf_end; p += sizeof(uint64_t)) {
        r->run_handles.emplace_back(leveldb::DecodeFixed64(p));
    }
    return Status::OK();
}

Status Segment::OpenMiniRun(int run_no, Block &index_block, MiniRun **run) {
    Rep *r = rep_;
    if (run_no < 0 || run_no >= r->run_handles.size())
        return Status::InvalidArgument("run_no is not in valid range");
    uint64_t run_offset = r->run_handles[run_no];
    uint64_t run_size =
            run_no + 1 == r->run_handles.size() ? r->file_size - run_offset : r->run_handles[run_no + 1] - run_offset;

    *run = new MiniRun(&r->options, r->file, run_offset, run_size, index_block);
    return Status::OK();
}


void Segment::ForEachRun(std::function<void (int, bool)> processor) {
    std::unordered_set<int> invalidated_runs;
    Rep *r = rep_;
    const char * start = r->invalidated_runs.data();
    const char * end = start + r->invalidated_runs.size();
    while (start < end) {
        uint32_t run_no;
        start = leveldb::GetVarint32Ptr(start, end, &run_no);
        invalidated_runs.insert(run_no);
    }

    for (size_t run_no = 0; run_no < r->run_handles.size(); ++run_no) {
        processor(run_no, invalidated_runs.find(run_no) != invalidated_runs.end());
    }
}

struct SegmentManager::Rep {
    std::mutex mutex;
    std::unordered_map<uint32_t, Segment*> segments;
    std::unordered_map<uint32_t, std::string> segment_filepaths;
    uint32_t seg_id_max = 0;
    Options options;
};

static bool GetSegmentFileInfo(const std::string & filename, uint32_t & seg_id) {
    if (filename.find("seg.") == 0) {
        std::string seg_id_str(filename.begin() + 4, filename.end());
        seg_id = std::stoi(seg_id_str);
        return true;
    }
    return false;
}

Status SegmentManager::NewSegmentBuilder(uint32_t *seg_id, silkstore::SegmentBuilder **seg_builder_ptr) {
    Rep*r = rep_;
    leveldb::Env* default_env = leveldb::Env::Default();
    std::lock_guard<std::mutex> g(r->mutex);
    uint32_t exp_seg_id = r->seg_id_max + 1;
    std::string src_segment_filepath = "tmpseg." + std::to_string(exp_seg_id);
    std::string target_segment_filepath = "seg." + std::to_string(exp_seg_id);
    WritableFile* wfile = nullptr;
    Status s = default_env->NewAppendableFile(src_segment_filepath, &wfile);
    if (!s.ok()) {
        return s;
    }
    *seg_builder_ptr = new SegmentBuilder(r->options, src_segment_filepath, target_segment_filepath, wfile);
    r->seg_id_max = *seg_id = exp_seg_id;
    return Status::OK();
}

Status SegmentManager::OpenSegment(uint32_t seg_id, silkstore::Segment **seg_ptr) {
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
        leveldb::Env* default_env = leveldb::Env::Default();
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
        s = Segment::Open(r->options, seg_id, rfile, filesize, seg_ptr);
        if (!s.ok()) {
            return s;
        }
        r->mutex.lock();
        r->segments[seg_id] = *seg_ptr;
    } else {
        *seg_ptr = it->second;
    }
    r->mutex.unlock();
    return Status::OK();
}

Status SegmentManager::OpenManager(const Options& options, const std::string& dbname, SegmentManager**manager_ptr) {
    leveldb::Env* default_env = leveldb::Env::Default();
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

}
