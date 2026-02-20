#pragma once

#include "bakread/backup_header.h"
#include "bakread/backup_stream.h"
#include "bakread/catalog_reader.h"
#include "bakread/decompressor.h"
#include "bakread/indexed_page_store.h"
#include "bakread/row_decoder.h"
#include "bakread/types.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// DirectExtractor -- Mode A: extract table data directly from .bak
//
// Orchestrates the full pipeline:
//   1. Parse backup header for metadata
//   2. Detect TDE/encryption (abort if found)
//   3. Stream pages from backup, decompress as needed
//   4. Build page cache (indexed by file:page)
//   5. Read system catalog to resolve table schema
//   6. Traverse data pages for the target table
//   7. Decode rows and emit to the callback
//
// Limitations (by design):
//   - Does NOT support TDE-encrypted databases
//   - Does NOT support backup-level encryption
//   - May fail on very complex schemas or unusual page layouts
//   - LOB data (MAX types) is not fully supported
//   - Not all SQL Server versions are handled identically
//
// When this mode fails, the caller should fall back to Mode B (restore).
// -------------------------------------------------------------------------

using RowCallback = std::function<bool(const Row& row)>;

struct DirectExtractResult {
    bool     success     = false;
    uint64_t rows_read   = 0;
    std::string error_message;
    bool     tde_detected = false;
    bool     encryption_detected = false;
};

struct TableInfo {
    std::string full_name;      // schema.table
    std::string schema_name;
    std::string table_name;
    int32_t     object_id = 0;
    int64_t     row_count = -1;  // -1 = unknown
    int64_t     page_count = -1; // -1 = unknown
};

struct ListTablesResult {
    bool success = false;
    std::vector<TableInfo> tables;
    std::string error_message;
};

// Configuration for indexed mode
struct DirectExtractorConfig {
    bool   use_indexed_mode = false;   // Use IndexedPageStore instead of in-memory cache
    size_t cache_size_mb = 256;        // LRU cache size in MB
    std::string index_dir;             // Directory for index files
    bool   force_rescan = false;       // Ignore existing index
};

class DirectExtractor {
public:
    explicit DirectExtractor(const std::vector<std::string>& bak_paths,
                            const DirectExtractorConfig& config = {});
    ~DirectExtractor();

    DirectExtractor(const DirectExtractor&) = delete;
    DirectExtractor& operator=(const DirectExtractor&) = delete;

    // Set the target table
    void set_table(const std::string& schema, const std::string& table);

    // Set column filter (empty = all columns)
    void set_columns(const std::vector<std::string>& columns);

    // Set max rows to extract (-1 = unlimited)
    void set_max_rows(int64_t max_rows);

    // Set allocation hints: only cache pages in this set (empty = cache all)
    void set_allocation_hints(std::unordered_set<int64_t> hints);

    // Set progress callback
    void set_progress_callback(ProgressCallback cb);

    // Check if using indexed mode
    bool is_indexed_mode() const { return config_.use_indexed_mode; }

    // Execute the extraction. Calls row_callback for each row.
    DirectExtractResult extract(RowCallback row_callback);

    // List all user tables in the backup
    ListTablesResult list_tables();

    // Get the resolved table schema (available after extract() runs)
    const TableSchema& resolved_schema() const { return schema_; }

    // Get backup info (available after extract() runs)
    const BackupInfo& backup_info() const;

private:
    // Phase 1: Parse headers, detect encryption
    bool phase_parse_headers();

    // Phase 2: Stream and cache pages from the backup
    bool phase_load_pages();

    // Phase 3: Build catalog and resolve table
    bool phase_resolve_table();

    // Phase 4: Extract rows
    uint64_t phase_extract_rows(RowCallback& callback);

    // Page provider for CatalogReader and data extraction
    bool provide_page(int32_t file_id, int32_t page_id, uint8_t* buf);

    // Cache a page from the backup stream
    void cache_page(int32_t file_id, int32_t page_id, const uint8_t* data);

    std::vector<std::string> bak_paths_;
    DirectExtractorConfig config_;
    std::string target_schema_;
    std::string target_table_;
    std::vector<std::string> target_columns_;
    int64_t     max_rows_ = -1;
    ProgressCallback progress_cb_ = nullptr;

    std::unique_ptr<BackupStream>       stream_;
    std::unique_ptr<BackupHeaderParser>  header_parser_;
    std::unique_ptr<Decompressor>        decompressor_;
    std::unique_ptr<CatalogReader>       catalog_;
    TableSchema                          schema_;

    // Indexed page store (for large backups)
    std::unique_ptr<IndexedPageStore>    indexed_store_;

    // Legacy in-memory page cache: key = (file_id << 32) | page_id
    std::unordered_map<int64_t, std::vector<uint8_t>> page_cache_;

    // Allocation hints: if non-empty, only cache pages in this set
    std::unordered_set<int64_t> allocation_hints_;

    static int64_t page_key(int32_t file_id, int32_t page_id) {
        return (static_cast<int64_t>(file_id) << 32) | static_cast<uint32_t>(page_id);
    }
};

}  // namespace bakread
