#pragma once

#include "bakread/lru_cache.h"
#include "bakread/page_index.h"
#include "bakread/decompressor.h"

#include <atomic>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace bakread {

// Progress callback for scan phase
// (pages_scanned, total_bytes_read, stripe_index)
using ScanProgressCallback = std::function<void(uint64_t, uint64_t, int)>;

// Configuration for indexed page store
struct IndexedStoreConfig {
    size_t cache_pages = 4096;          // LRU cache size (default 32MB)
    size_t scan_chunk_size = 65536;     // 64KB read chunks (8 pages)
    size_t num_threads = 0;             // 0 = auto (num cores)
    std::string index_dir;              // Directory for index files (empty = temp dir)
    bool force_rescan = false;          // Ignore existing index files
    bool save_index = true;             // Persist index to disk
};

// Manages parallel scanning of backup stripes and on-demand page access
class IndexedPageStore {
public:
    IndexedPageStore(const std::vector<std::string>& bak_paths,
                     const IndexedStoreConfig& config = {});
    ~IndexedPageStore();

    IndexedPageStore(const IndexedPageStore&) = delete;
    IndexedPageStore& operator=(const IndexedPageStore&) = delete;

    // Phase 1: Scan all stripes and build page index
    // This can be called explicitly or happens automatically on first get_page()
    bool scan(ScanProgressCallback progress = nullptr);

    // Check if index is ready
    bool is_indexed() const { return indexed_.load(); }

    // Get a page by file_id and page_id
    // Returns true if page was found, false otherwise
    bool get_page(int32_t file_id, int32_t page_id, uint8_t* out_buffer);

    // Get page index (for catalog building, allocation unit queries)
    const PageIndex& index() const { return index_; }

    // Statistics
    uint64_t pages_scanned() const { return pages_scanned_.load(); }
    uint64_t bytes_read() const { return bytes_read_.load(); }
    size_t cache_size() const { return cache_.size(); }
    double cache_hit_rate() const { return cache_.hit_rate(); }

    // Check if backup appears compressed
    bool is_compressed() const { return is_compressed_; }

    // Get the data start offset (after MTF headers)
    uint64_t data_start_offset() const { return data_start_offset_; }

private:
    // Scan a single stripe file (called from worker thread)
    void scan_stripe(int stripe_index, ScanProgressCallback progress);

    // Parse page header to extract metadata
    IndexedPageType classify_page(const uint8_t* page_data, uint32_t& out_object_id);

    // Read a page directly from stripe file
    bool read_page_from_stripe(const PageIndexEntry& entry, uint8_t* out_buffer);

    // Generate index file path for persistence
    std::string index_file_path() const;

    std::vector<std::string> bak_paths_;
    IndexedStoreConfig config_;

    PageIndex index_;
    LRUPageCache cache_;

    // Per-stripe file handles (opened lazily)
    std::vector<std::unique_ptr<std::ifstream>> stripe_files_;
    std::vector<std::mutex> stripe_mutexes_;

    // Decompressor for compressed backups
    std::unique_ptr<Decompressor> decompressor_;
    bool is_compressed_ = false;

    // Scan state
    std::atomic<bool> indexed_{false};
    std::atomic<uint64_t> pages_scanned_{0};
    std::atomic<uint64_t> bytes_read_{0};
    uint64_t data_start_offset_ = 0;

    // Thread pool for parallel scanning
    std::vector<std::thread> scan_threads_;
};

}  // namespace bakread
