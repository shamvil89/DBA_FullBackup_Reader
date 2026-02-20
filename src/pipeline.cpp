#include "bakread/pipeline.h"
#include "bakread/direct_extractor.h"
#include "bakread/restore_adapter.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <chrono>
#include <fstream>
#include <sstream>
#include <unordered_set>

namespace bakread {

// =========================================================================
// Helper: load allocation hints from CSV
// =========================================================================

static std::unordered_set<int64_t> load_allocation_hints(const std::string& path) {
    std::unordered_set<int64_t> hints;
    std::ifstream file(path);
    if (!file.is_open()) {
        LOG_WARN("Could not open allocation hint file: %s", path.c_str());
        return hints;
    }

    std::string line;
    bool header_skipped = false;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        // Skip header row if it contains non-numeric values
        if (!header_skipped) {
            if (line.find("file_id") != std::string::npos ||
                line.find("page_id") != std::string::npos ||
                line.find("FILE_ID") != std::string::npos ||
                line.find("PAGE_ID") != std::string::npos) {
                header_skipped = true;
                continue;
            }
            header_skipped = true;
        }

        // Parse CSV: file_id,page_id (may have quotes)
        std::istringstream ss(line);
        std::string file_id_str, page_id_str;

        if (!std::getline(ss, file_id_str, ',')) continue;
        if (!std::getline(ss, page_id_str, ',')) continue;

        // Remove quotes if present
        auto strip_quotes = [](std::string& s) {
            if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
                s = s.substr(1, s.size() - 2);
            }
        };
        strip_quotes(file_id_str);
        strip_quotes(page_id_str);

        try {
            int32_t file_id = std::stoi(file_id_str);
            int32_t page_id = std::stoi(page_id_str);
            int64_t key = (static_cast<int64_t>(file_id) << 32) |
                          static_cast<uint32_t>(page_id);
            hints.insert(key);
        } catch (...) {
            // Skip malformed lines
        }
    }

    LOG_INFO("Loaded %zu allocation hints from %s", hints.size(), path.c_str());
    return hints;
}

// =========================================================================
// RowQueue
// =========================================================================

RowQueue::RowQueue(size_t capacity)
    : capacity_(capacity)
{
}

bool RowQueue::push(Row row) {
    std::unique_lock<std::mutex> lk(mu_);
    not_full_.wait(lk, [&] { return queue_.size() < capacity_ || finished_; });
    if (finished_) return false;
    queue_.push(std::move(row));
    lk.unlock();
    not_empty_.notify_one();
    return true;
}

bool RowQueue::pop(Row& row) {
    std::unique_lock<std::mutex> lk(mu_);
    not_empty_.wait(lk, [&] { return !queue_.empty() || finished_; });
    if (queue_.empty()) return false;
    row = std::move(queue_.front());
    queue_.pop();
    lk.unlock();
    not_full_.notify_one();
    return true;
}

void RowQueue::finish() {
    std::lock_guard<std::mutex> lk(mu_);
    finished_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
}

size_t RowQueue::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return queue_.size();
}

// =========================================================================
// Pipeline
// =========================================================================

Pipeline::Pipeline(const Options& opts)
    : opts_(opts)
{
}

Pipeline::~Pipeline() = default;

PipelineResult Pipeline::run() {
    auto start_time = std::chrono::steady_clock::now();
    PipelineResult result;

    LOG_INFO("========================================");
    LOG_INFO("bakread - SQL Server Backup Table Extractor");
    LOG_INFO("========================================");
    if (opts_.bak_paths.size() == 1) {
        LOG_INFO("Backup:  %s", opts_.bak_paths[0].c_str());
    } else {
        LOG_INFO("Backup:  %zu striped files:", opts_.bak_paths.size());
        for (size_t i = 0; i < opts_.bak_paths.size(); ++i) {
            LOG_INFO("  [%zu] %s", i + 1, opts_.bak_paths[i].c_str());
        }
    }
    LOG_INFO("Table:   %s.%s", opts_.schema_name.c_str(), opts_.table_name.c_str());
    LOG_INFO("Output:  %s", opts_.output_path.c_str());
    LOG_INFO("Format:  %s",
             opts_.format == OutputFormat::CSV ? "CSV" :
             opts_.format == OutputFormat::Parquet ? "Parquet" : "JSONL");
    LOG_INFO("Mode:    %s",
             opts_.mode == ExecMode::Auto ? "auto" :
             opts_.mode == ExecMode::Direct ? "direct" : "restore");
    if (!opts_.allocation_hint_path.empty()) {
        LOG_INFO("Hints:   %s", opts_.allocation_hint_path.c_str());
    }
    LOG_INFO("========================================");

    switch (opts_.mode) {
    case ExecMode::Direct:
        result = try_direct_mode();
        break;

    case ExecMode::Restore:
        result = try_restore_mode();
        break;

    case ExecMode::Auto:
    default:
        LOG_INFO("Auto mode: attempting direct parse first...");
        result = try_direct_mode();

        if (!result.success) {
            LOG_INFO("Direct mode failed: %s", result.error_message.c_str());
            LOG_INFO("Falling back to restore mode...");

            if (opts_.target_server.empty()) {
                LOG_ERROR("Restore mode requires --target-server. "
                          "Specify a SQL Server instance to restore to.");
                result.error_message =
                    "Direct mode failed and no --target-server specified for "
                    "restore fallback. " + result.error_message;
            } else {
                result = try_restore_mode();
            }
        }
        break;
    }

    auto end_time = std::chrono::steady_clock::now();
    result.elapsed_seconds = std::chrono::duration<double>(
        end_time - start_time).count();

    LOG_INFO("========================================");
    if (result.success) {
        LOG_INFO("SUCCESS: %llu rows exported to %s",
                 (unsigned long long)result.rows_exported,
                 opts_.output_path.c_str());
        LOG_INFO("Mode:    %s", result.mode_used.c_str());
        LOG_INFO("Time:    %.2f seconds", result.elapsed_seconds);
    } else {
        LOG_ERROR("FAILED: %s", result.error_message.c_str());
    }
    LOG_INFO("========================================");

    return result;
}

PipelineResult Pipeline::try_direct_mode() {
    PipelineResult result;
    result.mode_used = "direct";

    try {
        // Configure extractor based on options
        DirectExtractorConfig config;
        config.use_indexed_mode = opts_.indexed_mode;
        config.cache_size_mb = opts_.cache_size_mb;
        config.index_dir = opts_.index_dir;
        config.force_rescan = opts_.force_rescan;

        DirectExtractor extractor(opts_.bak_paths, config);
        extractor.set_table(opts_.schema_name, opts_.table_name);
        extractor.set_columns(opts_.columns);
        extractor.set_max_rows(opts_.max_rows);

        if (config.use_indexed_mode) {
            result.mode_used = "direct (indexed)";
        }

        // Load allocation hints if provided
        if (!opts_.allocation_hint_path.empty()) {
            auto hints = load_allocation_hints(opts_.allocation_hint_path);
            if (!hints.empty()) {
                extractor.set_allocation_hints(std::move(hints));
            }
        }

        extractor.set_progress_callback([this](const Progress& p) {
            report_progress(p.rows_exported, p.pct);
        });

        // Create the writer
        auto writer = create_writer(opts_.format, opts_.delimiter);

        // Set up row queue for pipelined writing
        RowQueue queue(10000);
        std::atomic<uint64_t> written{0};
        std::atomic<bool> write_error{false};

        // We need the schema before opening the writer, so we use a
        // two-phase approach: extract writes to queue, writer reads from queue.

        // Phase: Extract rows, collect schema, write output
        bool schema_set = false;
        TableSchema schema;

        auto extract_result = extractor.extract([&](const Row& row) -> bool {
            if (!schema_set) {
                schema = extractor.resolved_schema();
                writer->open(opts_.output_path, schema);
                schema_set = true;
            }

            if (!writer->write_row(row)) {
                LOG_ERROR("Write failed at row %llu",
                          (unsigned long long)writer->rows_written());
                return false;
            }

            if (writer->rows_written() % 100000 == 0) {
                report_progress(writer->rows_written(), 0);
            }
            return true;
        });

        if (schema_set) {
            writer->close();
        }

        result.success = extract_result.success;
        result.rows_exported = extract_result.rows_read;
        result.error_message = extract_result.error_message;

    } catch (const std::exception& e) {
        result.error_message = e.what();
    }

    return result;
}

PipelineResult Pipeline::try_restore_mode() {
    PipelineResult result;
    result.mode_used = "restore";

    if (opts_.target_server.empty()) {
        result.error_message = "Restore mode requires --target-server";
        return result;
    }

    try {
        RestoreOptions ropts;
        ropts.bak_paths          = opts_.bak_paths;
        ropts.target_server      = opts_.target_server;
        ropts.sql_username       = opts_.sql_username;
        ropts.sql_password       = opts_.sql_password;
        ropts.schema_name        = opts_.schema_name;
        ropts.table_name         = opts_.table_name;
        ropts.columns            = opts_.columns;
        ropts.where_clause       = opts_.where_clause;
        ropts.max_rows           = opts_.max_rows;
        ropts.backupset          = opts_.backupset;
        ropts.tde_cert_pfx       = opts_.tde_cert_pfx;
        ropts.tde_cert_key       = opts_.tde_cert_key;
        ropts.tde_cert_password  = opts_.tde_cert_password;
        ropts.master_key_password = opts_.master_key_password;
        ropts.cleanup_keys       = opts_.cleanup_keys;

        RestoreAdapter adapter(ropts);

        auto writer = create_writer(opts_.format, opts_.delimiter);
        bool writer_opened = false;

        auto restore_result = adapter.extract([&](const Row& row) -> bool {
            if (!writer_opened) {
                writer->open(opts_.output_path, adapter.resolved_schema());
                writer_opened = true;
            }

            if (!writer->write_row(row)) {
                LOG_ERROR("Write failed at row %llu",
                          (unsigned long long)writer->rows_written());
                return false;
            }

            if (writer->rows_written() % 100000 == 0) {
                report_progress(writer->rows_written(), 0);
            }
            return true;
        });

        if (writer_opened) {
            writer->close();
        }

        result.success       = restore_result.success;
        result.rows_exported = restore_result.rows_read;
        result.error_message = restore_result.error_message;

    } catch (const std::exception& e) {
        result.error_message = e.what();
    }

    return result;
}

void Pipeline::writer_thread_func(IExportWriter* writer,
                                   RowQueue& queue,
                                   std::atomic<uint64_t>& written,
                                   std::atomic<bool>& error_flag) {
    Row row;
    while (queue.pop(row)) {
        if (error_flag.load()) break;

        if (!writer->write_row(row)) {
            error_flag.store(true);
            LOG_ERROR("Writer error at row %llu", (unsigned long long)written.load());
            break;
        }

        written.fetch_add(1);
    }
}

void Pipeline::report_progress(uint64_t rows, double pct) {
    if (pct > 0) {
        LOG_INFO("Progress: %.1f%% | %llu rows exported", pct,
                 (unsigned long long)rows);
    } else {
        LOG_INFO("Progress: %llu rows exported", (unsigned long long)rows);
    }
}

}  // namespace bakread
