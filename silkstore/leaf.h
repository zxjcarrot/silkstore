//
// Created by zxjcarrot on 2019-12-13.
//

#ifndef SILKSTORE_LEAF_H
#define SILKSTORE_LEAF_H

#include <stdint.h>
#include <functional>
#include <memory>
#include "leveldb/slice.h"
#include "table/block.h"

#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

class LeafManager;
static std::string MakeLeafFileName(uint32_t leaf_id);

class LeafAppender {
 public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    LeafAppender(const Options &options, WritableFile *file, size_t file_size,
                 uint32_t leaf_id, LeafManager *leaf_mgr);

    LeafAppender(const LeafAppender &) = delete;

    void operator=(const LeafAppender &) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~LeafAppender();

    uint32_t LeafId() const;

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    void Add(const Slice &key, const Slice &value);

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Starts a minirun building process.
    Status StartMiniRun();

    // Finish building a minirun.
    // Store the pointer to the current minirun in run_handle.
    // REQUIRES: Finish(), Abandon() have not been called.
    Status FinishMiniRun(MiniRunHandle *run_handle);

    // Return the index block for the previously finished run.
    // REQUIRES: FinishMiniRun() has been called and StartMiniRun() has not.
    Slice GetFinishedRunIndexBlock();

    // Return the filter block for the previously finished run.
    // REQUIRES: FinishMiniRun() has been called and StartMiniRun() has not.
    Slice GetFinishedRunFilterBlock();

    // Finish building the leaf.
    // REQUIRES: all mini runs has finished building through pairs of StartMiniRun() and FinishMiniRun().
    Status Finish();

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.  If invoked after a successful
    // Finish() call, returns the size of the final generated file.
    uint64_t FileSize() const;

    bool RunStarted() const;

    uint32_t GetFinishedRunDataSize();
 private:
    bool ok() const { return status().ok(); }

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.
    // REQUIRES: FinishMiniRun() has not been called
    void flush();

    struct Rep;
    Rep *rep_;
};

/*
* A leaf is the storage management unit of a range-based partition, typically sized in MBs.
* Writes to leave are append only and in unit of minirun.
* Leaf File Format:
*     run[n]
*/
class Leaf {
 public:
    static Status Open(const Options &options, uint32_t leaf_id,
                       RandomAccessFile *file, uint64_t file_size,
                       Leaf ** leaf);

    ~Leaf();

    Leaf(const Leaf &) = delete;

    void Ref();
    void UnRef();
    int NumRef();

    void operator=(const Leaf &) = delete;

    RandomAccessFile* SetNewLeafFile(RandomAccessFile* file);

    Status OpenMiniRun(MiniRunHandle run_handle, Block &index_block, MiniRun **run);

    // Iterate over all run numbers using a user-defined handler.
    // Arguments include a run number, handle to the run, size of the run, and a boolean value indicating
    // whether the run number has been invalidated through InvalidateMiniRun() previously.
    // This function is mainly used in garbage collection.
    void ForEachRun(std::function<bool(int run_no, MiniRunHandle run_handle, size_t run_size, bool valid)> processor);

    uint32_t LeafId() const;

    size_t LeafSize() const;
 private:
    struct Rep;
    Rep *rep_;

    explicit Leaf(Rep *rep) { rep_ = rep; }
};

class LeafManager {
 public:
    LeafManager(const LeafManager &) = delete;

    LeafManager(const LeafManager &&) = delete;

    LeafManager &operator=(const LeafManager &) = delete;

    LeafManager &operator=(const LeafManager &&) = delete;

    static Status OpenManager(const Options &options,
                              const std::string &dbname,
                              LeafManager **manager_ptr);

    // Open or create a leaf object
    // OpenLeaf should always be paired with DropLeaf
    Status OpenLeaf(uint32_t leaf_id, Leaf **leaf_Ptr);

    void DropLeaf(Leaf *leaf_ptr);

    // Remove leaf objects and underlying leaf files if associated.
    // This function should be called for phyiscal cleanup after GC.
    // This function waits for old readers to the leaf to exit before physically deleting resources.
    Status RemoveLeaf(uint32_t leaf_id);

    Status NewLeafAppender(uint32_t *leaf_id, std::unique_ptr<LeafAppender> &leaf_appender_ptr);

    Status NewLeafAppenderFromExistingLeaf(uint32_t leaf_id, std::unique_ptr<LeafAppender> &leaf_appender_ptr);
    size_t ApproximateSize();

    void ForEachLeaf(std::function<void(Leaf* leaf)> processor);

    Status SynchronizeLeafMetaWithPersistentData(uint32_t leaf_id);
 private:
    struct Rep;
    Rep *rep_;

    LeafManager(Rep *r) : rep_(r) {}
};

}  // namespace silkstore
}  // namespace leveldb

#endif //SILKSTORE_LEAF_H
