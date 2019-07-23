//
// Created by zxjcarrot on 2019-07-04.
//

#include <string>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include "silkstore/segment.h"
#include "silkstore/minirun.h"

namespace silkstore {

using namespace leveldb;

struct MiniRunBuilder::Rep {
    Options options;
    Options index_block_options;
    WritableFile *file;
    silkstore::SegmentBuilder *seg_builder;
    uint64_t start_offset; // records the start of a minirun and reduces the size of a BlockHandle after varint encoding.
    uint64_t offset;
    Status status;
    BlockBuilder data_block;
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    FilterBlockBuilder *filter_block;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    //
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    bool pending_index_entry;
    BlockHandle pending_handle;  // Handle to add to index block

    std::string compressed_output;

    Slice finished_index_block;
    Slice finished_filter_block;

    Rep(const Options &opt, WritableFile *f, uint64_t offset = 0)
        : options(opt),
          index_block_options(opt),
          file(f),
          start_offset(offset),
          offset(offset),
          data_block(&options),
          index_block(&index_block_options),
          num_entries(0),
          filter_block((opt.filter_policy == nullptr)
            ? nullptr : new FilterBlockBuilder(opt.filter_policy)),
          pending_index_entry(false) {
        index_block_options.block_restart_interval = 1;
    }
};


MiniRunBuilder::MiniRunBuilder(const Options &options,
                               WritableFile *file,
                               uint64_t file_offset)
        : rep_(new Rep(options, file, file_offset)) {
    if (rep_->filter_block != nullptr) {
        rep_->filter_block->StartBlock(0);
    }
}

MiniRunBuilder::~MiniRunBuilder() {
    delete rep_->filter_block;
    delete rep_;
}

void MiniRunBuilder::Add(const Slice &key, const Slice &value) {
    Rep *r = rep_;
    if (!ok()) return;
    if (r->num_entries > 0) {
        assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
    }

    if (r->pending_index_entry) {
        assert(r->data_block.empty());
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        r->pending_index_entry = false;
    }

    if (r->filter_block != nullptr) {
        r->filter_block->AddKey(key);
    }

    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    if (estimated_block_size >= r->options.block_size) {
        Flush();
    }
}

void MiniRunBuilder::Flush() {
    Rep *r = rep_;
    if (!ok()) return;
    if (r->data_block.empty()) return;
    assert(!r->pending_index_entry);
    WriteBlock(&r->data_block, &r->pending_handle);
    // We store offset to the start of the run in pending_handle to enable high compression rate.
    r->pending_handle.set_offset(r->pending_handle.offset() - r->start_offset);
    if (ok()) {
        r->pending_index_entry = true;
        r->status = r->file->Flush();
    }
    if (r->filter_block != nullptr) {
        //r->filter_block->StartBlock(r->offset);
    }
}

void MiniRunBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    assert(ok());
    Rep *r = rep_;
    Slice raw = block->Finish();

    Slice block_contents;
    CompressionType type = r->options.compression;
    // TODO(postrelease): Support more compression options: zlib?
    switch (type) {
        case leveldb::kNoCompression:
            block_contents = raw;
            break;

        case leveldb::kSnappyCompression: {
            std::string *compressed = &r->compressed_output;
            if (leveldb::port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = leveldb::kNoCompression;
            }
            break;
        }
    }
    WriteRawBlock(block_contents, type, handle);
    r->compressed_output.clear();
    block->Reset();
}

void MiniRunBuilder::WriteRawBlock(const Slice &block_contents,
                                   CompressionType type,
                                   BlockHandle *handle) {
    Rep *r = rep_;
    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());
    r->status = r->file->Append(block_contents);
    if (r->status.ok()) {
        char trailer[leveldb::kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc = leveldb::crc32c::Value(block_contents.data(), block_contents.size());
        crc = leveldb::crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
        leveldb::EncodeFixed32(trailer + 1, leveldb::crc32c::Mask(crc));
        r->status = r->file->Append(Slice(trailer, leveldb::kBlockTrailerSize));
        if (r->status.ok()) {
            r->offset += block_contents.size() + leveldb::kBlockTrailerSize;
        }
    }
}

Status MiniRunBuilder::status() const {
    return rep_->status;
}

Status MiniRunBuilder::Finish() {
    Rep *r = rep_;
    Flush();
    //BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

    // Finalize filter block
    if (ok() && r->filter_block != nullptr) {
        r->finished_filter_block = r->filter_block->FinishWithoutOffsets();
        //WriteRawBlock(r->filter_block->Finish(), leveldb::kNoCompression, &filter_block_handle);
    }

//    //silkstore's minirun has no metablock
//
//    // Write metaindex block
//    if (ok()) {
//        BlockBuilder meta_index_block(&r->options);
//        if (r->filter_block != nullptr) {
//            // Add mapping from "filter.Name" to location of filter data
//            std::string key = "filter.";
//            key.append(r->options.filter_policy->Name());
//            std::string handle_encoding;
//            filter_block_handle.EncodeTo(&handle_encoding);
//            meta_index_block.Add(key, handle_encoding);
//        }
//
//        // TODO(postrelease): Add stats and other meta blocks
//        WriteBlock(&meta_index_block, &metaindex_block_handle);
//    }

    // Finalize index block
    if (ok()) {
        if (r->pending_index_entry) {
            r->options.comparator->FindShortSuccessor(&r->last_key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->pending_index_entry = false;
        }
        // WriteBlock(&r->index_block, &index_block_handle);
        // silkstore's index block is stored in leaf index (possibly compressed)
        r->finished_index_block = r->index_block.Finish();
    }

//    // silkstore's minirun has no footer
//
//    // Write footer
//    if (ok()) {
//        leveldb::Footer footer;
//        footer.set_metaindex_handle(metaindex_block_handle);
//        footer.set_index_handle(index_block_handle);
//        std::string footer_encoding;
//        footer.EncodeTo(&footer_encoding);
//        r->status = r->file->Append(footer_encoding);
//        if (r->status.ok()) {
//            r->offset += footer_encoding.size();
//        }
//    }
    return r->status;
}

void MiniRunBuilder::Reset(uint64_t file_offset) {
    Rep *r = rep_;
    r->start_offset = file_offset;
    r->offset = file_offset;
    r->num_entries = 0;
    r->finished_index_block = Slice();
    r->finished_filter_block = Slice();
    r->status = Status::OK();
    r->last_key.clear();
    r->compressed_output.clear();
    r->pending_index_entry = false;
    r->data_block.Reset();
    r->index_block.Reset();
    r->filter_block->Reset();
    r->filter_block->StartBlock(0);
}

uint64_t MiniRunBuilder::NumEntries() const {
    return rep_->num_entries;
}

uint64_t MiniRunBuilder::FileSize() const {
    return rep_->offset;
}

}