//
// Created by zxjcarrot on 2019-07-02.
//

#include <string>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include "silkstore/segment.h"
#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

struct SegmentBuilder::Rep {
    Options options;
    WritableFile *file;
    uint64_t num_entries;
    MiniRunBuilder *run_builder;
    bool run_started;
    uint64_t prev_file_size;
    std::vector<MiniRunHandle> run_handles;
    Status status;
    std::string src_segment_filepath;
    std::string target_segment_filepath;

    Rep(const Options &opt, const std::string &src_segment_filepath,
        const std::string &target_segment_filepath, WritableFile *f)
        : options(opt),
          file(f),
          num_entries(0),
          run_builder(new MiniRunBuilder(opt, f, 0)),
          run_started(false),
          prev_file_size(0),
          src_segment_filepath(src_segment_filepath),
          target_segment_filepath(target_segment_filepath) {}

    ~Rep() {
        delete file;
    }
};

SegmentBuilder::SegmentBuilder(const Options &options, const std::string &src_segment_filepath,
                               const std::string &target_segment_filepath, WritableFile *file)
        : rep_(new Rep(options, src_segment_filepath, target_segment_filepath, file)) {
}

SegmentBuilder::~SegmentBuilder() {
    delete rep_->run_builder;
    delete rep_;
}

Status SegmentBuilder::StartMiniRun() {
    Rep *r = rep_;
    assert(r->run_started == false);
    r->run_started = true;
    r->run_builder->Reset(r->prev_file_size);
    return Status::OK();
}

Slice SegmentBuilder::GetFinishedRunIndexBlock() {
    Rep *r = rep_;
    assert(r->run_started == false);
    return r->run_builder->IndexBlock();
}

Slice SegmentBuilder::GetFinishedRunFilterBlock() {
    Rep *r = rep_;
    assert(r->run_started == false);
    return r->run_builder->FilterBlock();
}

Status SegmentBuilder::FinishMiniRun(uint32_t * run_no) {
    Rep *r = rep_;
    assert(r->run_started == true);
    r->status = r->run_builder->Finish();
    if (!ok()) return status();
    *run_no = r->run_handles.size();
    r->run_handles.push_back(r->prev_file_size);
    r->prev_file_size = r->run_builder->FileSize();
    r->run_started = false;
    return Status::OK();
}


void SegmentBuilder::Add(const Slice &key, const Slice &value) {
    Rep *r = rep_;
    assert(r->run_started);
    if (!ok()) return;
    r->run_builder->Add(key, value);
    r->status = r->run_builder->status();
    if (ok()) ++r->num_entries;
}

Status SegmentBuilder::status() const {
    return rep_->status;
}

Status SegmentBuilder::Finish() {
    Rep *r = rep_;

    /* TODO: Write out handles to the runs */
    std::string buf;

    for (auto handle : r->run_handles) {
        PutFixed64(&buf, handle);
    }
    size_t buf_size = buf.size();
    r->status = r->file->Append(buf);
    if (!ok()) return status();
    buf.clear();
    PutFixed64(&buf, buf_size);
    r->status = r->file->Append(buf);

    return Env::Default()->RenameFile(r->src_segment_filepath, r->target_segment_filepath);
}

uint64_t SegmentBuilder::NumEntries() const {
    return rep_->num_entries;
}

uint64_t SegmentBuilder::FileSize() const {
    return rep_->run_builder->FileSize();
}

}  // namespace silkstore
}  // namespace leveldb
