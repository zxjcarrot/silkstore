//
// Created by zxjcarrot on 2019-12-13.
//

#include "silkstore/leaf.h"

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

static std::string MakeLeafFileName(uint32_t leaf_id) {
    return "leaf." + std::to_string(leaf_id);
}

struct Leaf::Rep {
    std::vector<MiniRunHandle> run_handles;
    uint32_t id;
    RandomAccessFile *file;
    uint64_t file_size;
    Options options;
    std::atomic<int> ref_cnt;

    Rep() : ref_cnt(0) {}
};

uint32_t Leaf::LeafId() const {
    Rep *r = rep_;
    return r->id;
}

size_t Leaf::LeafSize() const {
    Rep *r = rep_;
    return r->file_size;
}

Leaf::~Leaf() {
    delete rep_;
    rep_ = nullptr;
}

Status Leaf::Open(const Options &options, uint32_t leaf_id,
                     RandomAccessFile *file, uint64_t file_size,
                     silkstore::Leaf **leaf) {
    Rep *r = new Rep;
    r->file = file;
    r->file_size = file_size;
    r->id = leaf_id;
    r->options = options;
    *leaf = new Leaf(r);

    return Status::OK();
}

RandomAccessFile *Leaf::SetNewLeafFile(RandomAccessFile *file) {
    Rep *r = rep_;
    assert(NumRef() == 0);
    auto old_file = r->file;
    r->file = file;
    return old_file;
}

Status Leaf::OpenMiniRun(MiniRunHandle run_handle, Block &index_block, MiniRun **run) {
    Rep *r = rep_;
    *run = new MiniRun(&r->options, r->file, run_handle.run_start_pos, run_handle.run_size, index_block);
    return Status::OK();
}

void Leaf::Ref() {
    Rep *r = rep_;
    r->ref_cnt.fetch_add(1);
}

void Leaf::UnRef() {
    Rep *r = rep_;
    r->ref_cnt.fetch_add(-1);
}

int Leaf::NumRef() {
    Rep *r = rep_;
    return r->ref_cnt.load();
}

void Leaf::ForEachRun(std::function<bool(int, MiniRunHandle, size_t, bool)> processor) {
    Rep *r = rep_;
    for (size_t run_no = 0; run_no < r->run_handles.size(); ++run_no) {
        bool valid = true;
        size_t run_size = run_no == r->run_handles.size() - 1 ? r->file_size - r->run_handles[run_no].run_start_pos :
                          r->run_handles[run_no + 1].run_start_pos - r->run_handles[run_no].run_start_pos;
        bool early_exit = processor(run_no, r->run_handles[run_no], run_size, valid);
        if (early_exit)
            break;
    }
}

struct LeafManager::Rep {
    std::mutex mutex;
    std::unordered_map<uint32_t, std::vector<MiniRunHandle>> leaf_run_handles;
    std::unordered_map<uint32_t, Leaf *> leaves;
    std::unordered_map<uint32_t, std::string> leaf_filepaths;
    uint32_t leaf_id_max = 0;
    Options options;
    std::string dbname;
};

static bool GetLeafFileInfo(const std::string &filename, uint32_t &leaf_id) {
    if (filename.find("leaf.") == 0) {
        std::string leaf_id_str(filename.begin() + 5, filename.end());
        leaf_id = std::stoi(leaf_id_str);
        return true;
    }
    return false;
}


void LeafManager::ForEachLeaf(std::function<void(Leaf *leaf)> processor) {
    Rep *r = rep_;
    std::unordered_set<uint32_t> leaf_ids;
    {
        std::lock_guard<std::mutex> g(r->mutex);
        for (auto kv : r->leaf_filepaths) {

            if (kv.second.find("tmpleaf.") == std::string::npos) {
                leaf_ids.insert(kv.first);
            }
        }
    }
    for (auto leaf_id : leaf_ids) {
        Leaf *leaf;
        Status s = OpenLeaf(leaf_id, &leaf);
        if (s.ok()) {
            processor(leaf);
            leaf->UnRef();
        }
    }
}


Status LeafManager::NewLeafAppender(uint32_t *leaf_id, std::unique_ptr<LeafAppender> &leaf_appender_ptr) {
    Rep *r = rep_;
    Env *default_env = Env::Default();

    std::lock_guard<std::mutex> g(r->mutex);
    uint32_t exp_leaf_id = r->leaf_id_max + 1;
    std::string leaf_filepath = r->dbname + "/leaf." + std::to_string(exp_leaf_id);
    WritableFile *wfile = nullptr;
    Status s = default_env->NewAppendableFile(leaf_filepath, &wfile);
    if (!s.ok()) {
        return s;
    }
    size_t file_size = 0;
    s = default_env->GetFileSize(leaf_filepath, &file_size);
    if (!s.ok()) {
        return s;
    }
    leaf_appender_ptr.reset(new LeafAppender(r->options, wfile, file_size, exp_leaf_id, this));
    r->leaf_id_max = *leaf_id = exp_leaf_id;
    r->leaf_filepaths[*leaf_id] = leaf_filepath;
    return Status::OK();
}

Status LeafManager::NewLeafAppenderFromExistingLeaf(uint32_t leaf_id, std::unique_ptr<LeafAppender> &leaf_appender_ptr) {
    Rep *r = rep_;
    Env *default_env = Env::Default();

    std::lock_guard<std::mutex> g(r->mutex);

    auto filepath_it = r->leaf_filepaths.find(leaf_id);
    if (filepath_it == r->leaf_filepaths.end()) {
        return Status::NotFound("leaf[" + std::to_string(leaf_id) + "] is not found");
    }

    std::string filepath = filepath_it->second;
    WritableFile *wfile = nullptr;
    Status s = default_env->NewAppendableFile(filepath, &wfile);
    if (!s.ok()) {
        return s;
    }
    size_t file_size = 0;
    s = default_env->GetFileSize(filepath, &file_size);
    if (!s.ok()) {
        return s;
    }
    leaf_appender_ptr.reset(new LeafAppender(r->options, wfile, file_size, leaf_id, this));

    return Status::OK();
}


Status LeafManager::SynchronizeLeafMetaWithPersistentData(uint32_t leaf_id) {
    Rep *r = rep_;
    Env *default_env = Env::Default();

    std::lock_guard<std::mutex> g(r->mutex);

    auto filepath_it = r->leaf_filepaths.find(leaf_id);
    if (filepath_it == r->leaf_filepaths.end()) {
        return Status::NotFound("leaf[" + std::to_string(leaf_id) + "] is not found");
    }

    auto it = r->leaves.find(leaf_id);
    if (it == r->leaves.end()) {
        // If the leaf has not been opened yet, don't do anything.
        // Because the leaf's meta will be updated on the next open.
        return Status::OK();
    } else {
        std::string filepath = filepath_it->second;
        Leaf *old_leaf = it->second;
        size_t latest_filesize;
        Status s = default_env->GetFileSize(filepath, &latest_filesize);
        if (!s.ok()) {
            return s;
        }
        RandomAccessFile *rfile;
        s = default_env->NewRandomAccessFile(filepath, &rfile);
        if (!s.ok()) {
            return s;
        }
        Leaf * new_leaf_ptr;
        s = Leaf::Open(r->options, leaf_id, rfile, latest_filesize, &new_leaf_ptr);
        if (!s.ok()) {
            delete rfile;
            return s;
        }
        r->leaves[leaf_id] = new_leaf_ptr;
        r->mutex.unlock();
        // Wait for all read references to this old_leaf to drop
        while (old_leaf->NumRef())
            default_env->SleepForMicroseconds(10);
        delete old_leaf;
        r->mutex.lock();
    }
    return Status::OK();
}

size_t LeafManager::ApproximateSize() {
    Rep *r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    size_t size = 0;
    Env *default_env = Env::Default();
    for (auto kv : r->leaf_filepaths) {
        auto filepath = kv.second;
        uint64_t filesize;
        Status s = default_env->GetFileSize(filepath, &filesize);
        if (s.ok()) {
            size += filesize;
        } else {
            Log(r->options.info_log, "Failed getting size of file %s\n", filepath.c_str());
        }
    }
    //Log(r->options.info_log, "Approximate size of all leaf files %lu\n", size);
    return size;
}

Status LeafManager::RemoveLeaf(uint32_t leaf_id) {
    Status s;
    Rep *r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    auto filepath_it = r->leaf_filepaths.find(leaf_id);
    if (filepath_it == r->leaf_filepaths.end()) {
        return Status::NotFound("leaf[" + std::to_string(leaf_id) + "] is not found");
    }

    auto it = r->leaves.find(leaf_id);
    if (it != r->leaves.end()) {
        std::string filepath = filepath_it->second;
        Leaf *leaf = it->second;
        r->leaves.erase(leaf_id);
        r->leaf_filepaths.erase(leaf_id);
        Env *default_env = Env::Default();
        r->mutex.unlock();
        // Wait for all read references to this leaf to drop
        while (leaf->NumRef())
            default_env->SleepForMicroseconds(10);
        delete leaf;
        s = default_env->DeleteFile(filepath);
        r->mutex.lock();
        if (!s.ok())
            return s;
    }
    return s;
}

void LeafManager::DropLeaf(Leaf *leaf_ptr) {
    leaf_ptr->UnRef();
}

Status LeafManager::OpenLeaf(uint32_t leaf_id, Leaf **leaf_ptr) {
    Rep *r = rep_;
    std::lock_guard<std::mutex> g(r->mutex);
    auto filepath_it = r->leaf_filepaths.find(leaf_id);
    if (filepath_it == r->leaf_filepaths.end()) {
        return Status::NotFound("leaf[" + std::to_string(leaf_id) + "] is not found");
    }

    auto it = r->leaves.find(leaf_id);
    if (it == r->leaves.end()) {
        std::string filepath = filepath_it->second;
        r->mutex.unlock();
        Env *default_env = Env::Default();
        RandomAccessFile *rfile;
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
        s = Leaf::Open(r->options, leaf_id, rfile, filesize, leaf_ptr);
        if (!s.ok()) {
            return s;
        }
        r->leaves[leaf_id] = *leaf_ptr;
        r->leaf_filepaths[leaf_id] = filepath;
    } else {
        *leaf_ptr = it->second;
    }
    (*leaf_ptr)->Ref();
    return Status::OK();
}

Status LeafManager::OpenManager(const Options &options,
                                   const std::string &dbname,
                                   LeafManager **manager_ptr) {
    Env *default_env = Env::Default();
    if (default_env->FileExists(dbname) == false) {
        if (options.create_if_missing == false) {
            return Status::NotFound("dbname[" + dbname + "] not found");
        }
        Status s = default_env->CreateDir(dbname);
        if (!s.ok()) {
            return s;
        }
    }
    Rep *r = new Rep;
    r->options = options;
    r->dbname = dbname;
    std::vector<std::string> subfiles;
    Status s = default_env->GetChildren(dbname, &subfiles);
    if (!s.ok()) {
        return s;
    }

    std::vector<std::string> leaf_files;
    std::vector<uint32_t> leaf_ids;
    for (auto filename : subfiles) {
        uint32_t leaf_id = -1;
        if (GetLeafFileInfo(filename, leaf_id)) {
            std::string filepath = dbname + "/" + filename;
            leaf_files.push_back(filepath);
            leaf_ids.push_back(leaf_id);
            r->leaf_filepaths[leaf_id] = filepath;
            r->leaf_id_max = std::max(r->leaf_id_max, leaf_id);
        }
    }

    *manager_ptr = new LeafManager(r);
    return Status::OK();
}

}  // namespace silkstore
}  // namespace leveldb