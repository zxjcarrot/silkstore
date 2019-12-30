//
// Created by zxjcarrot on 2019-12-15.
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

#include "silkstore/leaf.h"
#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

struct LeafAppender::Rep {
    Options options;
    WritableFile *file;
    uint64_t num_entries;
    MiniRunBuilder *run_builder;
    bool run_started;
    uint64_t prev_file_size;
    std::vector<MiniRunHandle> run_handles;
    Status status;
    uint32_t leaf_id;
    LeafManager *leaf_mgr;

    Rep(const Options &opt, WritableFile *f, size_t file_size,
        uint32_t leaf_id, LeafManager *leaf_mgr)
            : options(opt),
              file(f),
              num_entries(0),
              run_builder(new MiniRunBuilder(opt, f, file_size)),
              run_started(false),
              prev_file_size(file_size),
              leaf_id(leaf_id), leaf_mgr(leaf_mgr) {}

    ~Rep() {
        delete run_builder;
        delete file;
    }
};

LeafAppender::LeafAppender(const Options &options, WritableFile *file, size_t file_size,
                               uint32_t leaf_id, LeafManager *leaf_mgr)
        : rep_(new Rep(options, file, file_size, leaf_id, leaf_mgr)) {
}

LeafAppender::~LeafAppender() {
    Finish();
    delete rep_;
}

uint32_t LeafAppender::LeafId() const {
    return rep_->leaf_id;
}

Status LeafAppender::StartMiniRun() {
    Rep *r = rep_;
    assert(r->run_started == false);
    r->run_started = true;
    r->run_builder->Reset(r->prev_file_size);
    return Status::OK();
}


bool LeafAppender::RunStarted() const {
    Rep *r = rep_;
    return r->run_started;
}

Slice LeafAppender::GetFinishedRunIndexBlock() {
    Rep *r = rep_;
    assert(r->run_started == false);
    return r->run_builder->IndexBlock();
}

uint32_t LeafAppender::GetFinishedRunDataSize() {
    Rep *r = rep_;
    assert(r->run_started == false);
    return r->run_builder->GetCurrentRunDataSize();
}

Slice LeafAppender::GetFinishedRunFilterBlock() {
    Rep *r = rep_;
    assert(r->run_started == false);
    return r->run_builder->FilterBlock();
}

Status LeafAppender::FinishMiniRun(MiniRunHandle *run_handle) {
    Rep *r = rep_;
    assert(r->run_started == true);
    r->status = r->run_builder->Finish();
    if (!ok()) return status();
    *run_handle =  MiniRunHandle{r->prev_file_size, r->run_builder->FileSize() - r->prev_file_size};
    r->run_handles.push_back(*run_handle);
    r->prev_file_size = r->run_builder->FileSize();
    r->run_started = false;
    return Status::OK();
}

void LeafAppender::Add(const Slice &key, const Slice &value) {
    Rep *r = rep_;
    assert(r->run_started);
    if (!ok()) return;
    r->run_builder->Add(key, value);
    r->status = r->run_builder->status();
    if (ok()) ++r->num_entries;
}

Status LeafAppender::status() const {
    return rep_->status;
}

Status LeafAppender::Finish() {
    Rep *r = rep_;
    return r->file->Flush();
}

uint64_t LeafAppender::NumEntries() const {
    return rep_->num_entries;
}

uint64_t LeafAppender::FileSize() const {
    return rep_->run_builder->FileSize();
}

}  // namespace silkstore
}  // namespace leveldb
