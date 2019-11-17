//
// Created by zxjcarrot on 2019-06-28.
//

#ifndef SILKSTORE_SEGMENT_H
#define SILKSTORE_SEGMENT_H

#include <stdint.h>
#include <functional>
#include <memory>
#include "leveldb/slice.h"
#include "table/block.h"

#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

class SegmentManager;
static std::string MakeSegmentFileName(uint32_t segment_id);

class SegmentBuilder {
 public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    SegmentBuilder(const Options &options, const std::string &src_segment_filepath,
                   const std::string &target_segment_filepath, WritableFile *file,
                   uint32_t seg_id, SegmentManager * segment_mgr);

    SegmentBuilder(const SegmentBuilder &) = delete;

    void operator=(const SegmentBuilder &) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~SegmentBuilder();

    uint32_t SegmentId() const;

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
    Status FinishMiniRun(uint32_t *run_no);

    // Return the index block for the previously finished run.
    // REQUIRES: FinishMiniRun() has been called and StartMiniRun() has not.
    Slice GetFinishedRunIndexBlock();

    // Return the filter block for the previously finished run.
    // REQUIRES: FinishMiniRun() has been called and StartMiniRun() has not.
    Slice GetFinishedRunFilterBlock();

    // Finish building the segment.
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
* A segment is the storage management unit and typically sized in MBs.
* Writes to segment are append only and in unit of minirun.
* GC runs at per-segment level.
* Segment File Format:
*     run[n]
*     run_handle[n]
*     n
*/
class Segment {
 public:
    static Status Open(const Options &options, uint32_t segment_id,
                       RandomAccessFile *file, uint64_t file_size,
                       Segment **segment);

    ~Segment();

    Segment(const Segment &) = delete;

    void Ref();
    void UnRef();
    int NumRef();

    void operator=(const Segment &) = delete;

    void SetNewSegmentFile(RandomAccessFile* file);

    Status OpenMiniRun(int run_no, Block &index_block, MiniRun **run);

    // Mark the minirun indicated by the segment.run_handle[run_no] as invalid.
    // Later GCs can simply skip this run without querying index for validness.
    Status InvalidateMiniRun(const int &run_no);

    // Iterate over all run numbers using a user-defined handler.
    // Arguments include a run number, handle to the run, size of the run, and a boolean value indicating
    // whether the run number has been invalidated through InvalidateMiniRun() previously.
    // This function is mainly used in garbage collection.
    void ForEachRun(std::function<bool(int run_no, MiniRunHandle run_handle, size_t run_size, bool valid)> processor);

    uint32_t SegmentId() const;
    size_t SegmentSize() const;
 private:
    struct Rep;
    Rep *rep_;

    explicit Segment(Rep *rep) { rep_ = rep; }
};

class SegmentManager {
 public:
    SegmentManager(const SegmentManager &) = delete;

    SegmentManager(const SegmentManager &&) = delete;

    SegmentManager &operator=(const SegmentManager &) = delete;

    SegmentManager &operator=(const SegmentManager &&) = delete;

    static Status OpenManager(const Options &options,
                              const std::string &dbname,
                              SegmentManager **manager_ptr,
                              std::function<void()> gc_func);

    // Get the top K most invalidated segments
    std::vector<Segment*> GetMostInvalidatedSegments(int K);

    // Open or create a segment object
    // OpenSegment should always be paired with DropSegment
    Status OpenSegment(uint32_t seg_id, Segment **seg_ptr);

    void DropSegment(Segment *seg_ptr);

    // Remove segment objects and underlying segment files if associated.
    // This function should be called for phyiscal cleanup after GC.
    // This function waits for old readers to the segment to exit before physically deleting resources.
    Status RemoveSegment(uint32_t seg_id);

    Status NewSegmentBuilder(uint32_t *seg_id, std::unique_ptr<SegmentBuilder> &seg_builder_ptr, bool gc_on_segment_shortage);

    size_t ApproximateSize();

    Status InvalidateSegmentRun(uint32_t seg_id, uint32_t run_no);

    Status RenameSegment(uint32_t seg_id, const std::string target_filepath);
 private:
    struct Rep;
    Rep *rep_;

    SegmentManager(Rep *r) : rep_(r) {}
};

}  // namespace silkstore
}  // namespace leveldb

#endif //SILKSTORE_SEGMENT_H
