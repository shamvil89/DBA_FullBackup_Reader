#pragma once

#include "bakread/cli.h"
#include "bakread/export_writer.h"
#include "bakread/types.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// Thread-safe bounded row queue for producer-consumer pipeline
// -------------------------------------------------------------------------
class RowQueue {
public:
    explicit RowQueue(size_t capacity = 10000);

    // Push a row. Blocks if queue is full. Returns false if done.
    bool push(Row row);

    // Pop a row. Blocks if queue is empty. Returns false if done and empty.
    bool pop(Row& row);

    // Signal that no more rows will be pushed
    void finish();

    // Get approximate queue size
    size_t size() const;

private:
    std::queue<Row>         queue_;
    size_t                  capacity_;
    mutable std::mutex      mu_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    bool                    finished_ = false;
};

// -------------------------------------------------------------------------
// Pipeline -- orchestrates the full extraction pipeline
//
//   [Reader Thread] --> [RowQueue] --> [Writer Thread]
//
// The reader thread can be either:
//   - DirectExtractor (Mode A)
//   - RestoreAdapter (Mode B)
//
// The writer thread writes to CSV/Parquet/JSONL.
// -------------------------------------------------------------------------

struct PipelineResult {
    bool        success     = false;
    uint64_t    rows_exported = 0;
    std::string mode_used;   // "direct" or "restore"
    std::string error_message;
    double      elapsed_seconds = 0.0;
};

class Pipeline {
public:
    explicit Pipeline(const Options& opts);
    ~Pipeline();

    // Run the full pipeline
    PipelineResult run();

private:
    // Mode A attempt
    PipelineResult try_direct_mode();

    // Mode B attempt
    PipelineResult try_restore_mode();

    // Writer thread entry point
    void writer_thread_func(IExportWriter* writer,
                            RowQueue& queue,
                            std::atomic<uint64_t>& written,
                            std::atomic<bool>& error_flag);

    // Progress reporting
    void report_progress(uint64_t rows, double pct);

    Options opts_;
};

}  // namespace bakread
