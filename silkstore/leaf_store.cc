//
// Created by zxjcarrot on 2019-07-15.
//
#include "silkstore/util.h"
#include "leaf_store.h"

#include "util/coding.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/merger.h"

namespace silkstore {


MiniRunIndexEntry::MiniRunIndexEntry(const Slice &data) : raw_data_(data) {
    assert(raw_data_.size() >= 16);
    const char *p = raw_data_.data();
    segment_number_ = leveldb::DecodeFixed32(p);
    p += 4;
    run_no_within_segment_ = leveldb::DecodeFixed32(p);
    p += 4;
    block_index_data_len_ = leveldb::DecodeFixed32(p);
    p += 4;
    filter_data_len_ = leveldb::DecodeFixed32(p);
}

void MiniRunIndexEntry::EncodeMiniRunIndexEntry(uint32_t seg_no, uint32_t run_no, Slice block_index_data, Slice filter_data, std::string * buf) {
    PutFixed32(buf, seg_no);
    PutFixed32(buf, run_no);
    PutFixed32(buf, block_index_data.size());
    PutFixed32(buf, filter_data.size());
    buf->append(block_index_data.data(), block_index_data.size());
    buf->append(filter_data.data(), filter_data.size());
}

Slice MiniRunIndexEntry::GetBlockIndexData() const {
    const char *p = raw_data_.data() + 16;
    return Slice(p, block_index_data_len_);
}

Slice MiniRunIndexEntry::GetFilterData() const {
    const char *p = raw_data_.data() + 16 + block_index_data_len_;
    return Slice(p, filter_data_len_);
}

LeafIndexEntry::LeafIndexEntry(const Slice &data) : raw_data_(data) {
    assert(raw_data_.size() >= 4);
}

uint32_t LeafIndexEntry::GetNumMiniRuns() const {
    if (raw_data_.empty())
        return 0;
    const char *p = raw_data_.data() + raw_data_.size() - 4;
    return leveldb::DecodeFixed32(p);
}

std::vector<MiniRunIndexEntry> LeafIndexEntry::GetAllMiniRunIndexEntry(TraversalOrder order) const {
    std::vector<MiniRunIndexEntry> res;
    auto processor = [&res](const MiniRunIndexEntry &entry, uint32_t) {
        res.push_back(entry);
        return false;
    };
    ForEachMiniRunIndexEntry(processor, order);
    return res;
}

void LeafIndexEntry::ForEachMiniRunIndexEntry(std::function<bool(const MiniRunIndexEntry &, uint32_t)> processor,
                                              TraversalOrder order) const {
    auto num_entries = GetNumMiniRuns();
    if (num_entries == 0)
        return;


    if (order == TraversalOrder::backward) {
        const char *p = raw_data_.data() + raw_data_.size() - 4;
        for (int i = num_entries - 1; i >= 0; ++i) {
            p -= 4;
            assert(p >= raw_data_.data());
            uint32_t entry_size = leveldb::DecodeFixed32(p);
            p -= entry_size;
            assert(p >= raw_data_.data());
            MiniRunIndexEntry index_entry = MiniRunIndexEntry(Slice(p, entry_size));
            bool early_return = processor(index_entry, num_entries - i);
            if (early_return)
                return;
        }
    } else {
        std::vector<MiniRunIndexEntry> entries;
        const char *p = raw_data_.data() + raw_data_.size() - 4;
        for (int i = num_entries - 1; i >= 0; --i) {
            p -= 4;
            assert(p >= raw_data_.data());
            uint32_t entry_size = leveldb::DecodeFixed32(p);
            p -= entry_size;
            assert(p >= raw_data_.data());
            MiniRunIndexEntry index_entry = MiniRunIndexEntry(Slice(p, entry_size));
            entries.push_back(index_entry);
        }

        for (int i = 0; i < entries.size(); ++i) {
            bool early_return = processor(entries[i], i);
            if (early_return)
                return;
        }
    }
}

Status
LeafIndexEntryBuilder::AppendMiniRunIndexEntry(const LeafIndexEntry &base,
                                               const MiniRunIndexEntry &minirun_index_entry,
                                               std::string *buf,
                                               LeafIndexEntry *new_entry) {
    buf->append(base.GetRawData().data(), base.GetRawData().size());
    if (buf->size()) {
        // Erase footer (# of minirun index entries).
        buf->resize(buf->size() - 4);
    }
    buf->append(minirun_index_entry.GetRawData().data(), minirun_index_entry.GetRawData().size());
    PutFixed32(buf, minirun_index_entry.GetRawData().size());
    // Append new footer (# of minirun index entries).
    PutFixed32(buf, base.GetNumMiniRuns() + 1);
    *new_entry = LeafIndexEntry(Slice(*buf));
    return Status::OK();
}

Status
LeafIndexEntryBuilder::ReplaceMiniRunRange(const LeafIndexEntry &base, uint32_t start, uint32_t end,
                                           const MiniRunIndexEntry &replacement,
                                           std::string *buf,
                                           LeafIndexEntry *new_entry) {
    if (start > base.GetNumMiniRuns() || end > base.GetNumMiniRuns())
        return Status::InvalidArgument(
                "[start, end] not within bound of [0, " + std::to_string(base.GetNumMiniRuns()) + "]");
    buf->clear();
    uint32_t new_num_entries = 0;
    auto processor = [&](const MiniRunIndexEntry &entry, uint32_t idx) {
        if (start <= idx && idx <= end) {
            if (idx == start) {
                buf->append(replacement.GetRawData().data(), replacement.GetRawData().size());
                PutFixed32(buf, replacement.GetRawData().size());
                ++new_num_entries;
            }
        } else {
            buf->append(entry.GetRawData().data(), entry.GetRawData().size());
            PutFixed32(buf, replacement.GetRawData().size());
            ++new_num_entries;
        }
        return false;
    };
    base.ForEachMiniRunIndexEntry(processor, LeafIndexEntry::TraversalOrder::forward);
    // Append footer (# of minirun index entries).
    PutFixed32(buf, base.GetNumMiniRuns() + 1);
    *new_entry = LeafIndexEntry(Slice(*buf));
    return Status::OK();
}

Iterator* LeafStore::NewIterator(const ReadOptions &options) {
    // TODO: implment iterator interface
}

Status LeafStore::Get(const ReadOptions &options, const LookupKey &key, std::string *value) {
    Iterator *it = leaf_index_->NewIterator(options);
    DeferCode c([it](){delete it;});
    it->Seek(key.user_key());
    if (it->Valid() == false)
        return Status::NotFound("");
    Slice index_data = it->value();
    Status s;
    LeafIndexEntry index_entry(index_data);

    auto processor = [&, this](const MiniRunIndexEntry &minirun_index_entry, uint32_t) -> bool {
        if (options_.filter_policy) {
            FilterBlockReader filter(options_.filter_policy, minirun_index_entry.GetFilterData());
            if (filter.KeyMayMatch(0, key.internal_key()) == false) {
                return false;
            }
        }
        uint32_t seg_no = minirun_index_entry.GetSegmentNumber();
        Segment *seg = nullptr;
        s = seg_manager_->OpenSegment(seg_no, &seg);
        if (!s.ok())
            return true;

        Block index_block(BlockContents{minirun_index_entry.GetBlockIndexData(), false, false});
        MiniRun *run;
        uint32_t run_no = minirun_index_entry.GetRunNumberWithinSegment();
        s = seg->OpenMiniRun(run_no, index_block, &run);
        if (!s.ok())
            return true;

        Iterator *iter = run->NewIterator(options);
        DeferCode c([iter, run](){delete run; delete iter;});
        iter->Seek(key.internal_key());

        if (iter->Valid()) {
            ParsedInternalKey parsed_key;
            if (!ParseInternalKey(iter->key(), &parsed_key)) {
                s = Status::Corruption("key corruption");
                return true;
            } else {
                if (user_cmp_->Compare(parsed_key.user_key, key.user_key()) == 0) {
                    if (parsed_key.type == kTypeValue) { // kFound
                        value->assign(iter->value().data(), iter->value().size());
                        s = Status::OK();
                    } else { // kDeleted
                        s = Status::NotFound("");
                    }
                    return true;
                }
            }
        }

        return false;
    };

    index_entry.ForEachMiniRunIndexEntry(processor, LeafIndexEntry::TraversalOrder::backward);
    return s;
}

static void NewIteratorForLeafCleanupFunc(void *arg1, void *) {
    delete static_cast<MiniRun *>(arg1);
}

Iterator *LeafStore::NewIteratorForLeaf(const ReadOptions &options, const LeafIndexEntry &leaf_index_entry, Status &s,
                                        uint32_t start_minirun_no, uint32_t end_minirun_no) {
    s = Status::OK();
    std::vector<Iterator *> iters;
    std::vector<MiniRun *> runs;
    iters.reserve(leaf_index_entry.GetNumMiniRuns());
    runs.reserve(leaf_index_entry.GetNumMiniRuns());
    auto processor = [&, this](const MiniRunIndexEntry &minirun_index_entry, uint32_t run_no) -> bool {
        if (run_no > end_minirun_no) {
            return true; // early return
        }

        if (start_minirun_no <= run_no && run_no <= end_minirun_no) {
            uint32_t seg_no = minirun_index_entry.GetSegmentNumber();
            Segment *seg = nullptr;
            s = seg_manager_->OpenSegment(seg_no, &seg);
            if (!s.ok())
                return true; // error, early return
            MiniRun *run;
            Block index_block(BlockContents{minirun_index_entry.GetBlockIndexData(), false, false});
            s = seg->OpenMiniRun(run_no, index_block, &run);
            if (!s.ok())
                return true; // error, early return
            Iterator *iter = run->NewIterator(options);
            iters.push_back(iter);
            runs.push_back(run);
        }

        return false;
    };
    leaf_index_entry.ForEachMiniRunIndexEntry(processor, LeafIndexEntry::TraversalOrder::forward);
    if (s.ok()) {
        return nullptr;
    }
    // Destroy miniruns opened when iterator is deleted by MergingIterator
    for (int i = 0; i < iters.size(); ++i) {
        iters[i]->RegisterCleanup(NewIteratorForLeafCleanupFunc, runs[i], nullptr);
    }
    return NewMergingIterator(options_.comparator, &iters[0], iters.size());
}

Status LeafStore::Open(SegmentManager *seg_manager, leveldb::DB *leaf_index, const Options &options,
                       const Comparator *user_cmp, LeafStore **store) {
    *store = new LeafStore(seg_manager, leaf_index, options, user_cmp);
    return Status::OK();
}

}