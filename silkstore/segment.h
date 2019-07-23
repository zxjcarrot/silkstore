//
// Created by zxjcarrot on 2019-06-28.
//

#ifndef SILKSTORE_SEGMENT_H
#define SILKSTORE_SEGMENT_H

#include <stdint.h>

#include "leveldb/slice.h"
#include "table/block.h"

#include "silkstore/minirun.h"

namespace leveldb {
namespace silkstore {

static std::string MakeSegmentFileName(uint32_t segment_id);

class SegmentBuilder {
 public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    SegmentBuilder(const Options &options, const std::string &src_segment_filepath,
                   const std::string &target_segment_filepath, WritableFile *file);

    SegmentBuilder(const SegmentBuilder &) = delete;

    void operator=(const SegmentBuilder &) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~SegmentBuilder();

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

    void operator=(const Segment &) = delete;

    Status OpenMiniRun(int run_no, Block &index_block, MiniRun **run);

    // Mark the minirun indicated by the segment.run_handle[run_no] as invalid.
    // Later GCs can simply skip this run without querying index for validness.
    Status InvalidateMiniRun(const int &run_no);

    // Iterate over all run numbers using a user-defined handler.
    // Arguments include a run number and a boolean value indicating
    // whether the run number has been invalidated before through InvalidateMiniRun() previously.
    // This function is mainly used in garbage collection.
    void ForEachRun(std::function<void(int run_no, bool in_invalidated_runs)> processor);

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
                              SegmentManager **manager_ptr);

    Status OpenSegment(uint32_t seg_id, Segment **seg_ptr);

    Status NewSegmentBuilder(uint32_t *seg_id, SegmentBuilder **seg_builder_ptr);

    size_t ApproximateSize();

 private:
    struct Rep;
    Rep *rep_;

    SegmentManager(Rep *r) : rep_(r) {}
};

}  // namespace silkstore
}  // namespace leveldb

#endif //SILKSTORE_SEGMENT_H
