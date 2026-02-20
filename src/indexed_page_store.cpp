#include "bakread/indexed_page_store.h"
#include "bakread/backup_header.h"
#include "bakread/backup_stream.h"
#include "bakread/error.h"
#include "bakread/logging.h"
#include "bakread/page.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>

namespace bakread {

namespace fs = std::filesystem;

IndexedPageStore::IndexedPageStore(const std::vector<std::string>& bak_paths,
                                   const IndexedStoreConfig& config)
    : bak_paths_(bak_paths)
    , config_(config)
    , cache_(config.cache_pages)
    , stripe_mutexes_(bak_paths.size())
{
    stripe_files_.resize(bak_paths.size());

    // Auto-detect thread count
    if (config_.num_threads == 0) {
        config_.num_threads = std::max(1u, std::thread::hardware_concurrency());
    }

    LOG_INFO("IndexedPageStore initialized: %zu stripes, %zu cache pages, %zu threads",
             bak_paths_.size(), config_.cache_pages, config_.num_threads);
}

IndexedPageStore::~IndexedPageStore() {
    // Wait for any running scan threads
    for (auto& t : scan_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

std::string IndexedPageStore::index_file_path() const {
    if (!config_.index_dir.empty()) {
        fs::path dir(config_.index_dir);
        fs::create_directories(dir);
        return (dir / "bakread_index.idx").string();
    }

    // Use temp directory next to first backup file
    fs::path bak_path(bak_paths_[0]);
    fs::path index_path = bak_path.parent_path() / (bak_path.stem().string() + "_bakread.idx");
    return index_path.string();
}

bool IndexedPageStore::scan(ScanProgressCallback progress) {
    if (indexed_.load()) {
        LOG_DEBUG("Index already built, skipping scan");
        return true;
    }

    // Try to load existing index
    if (!config_.force_rescan) {
        std::string idx_path = index_file_path();
        if (index_.load_from_file(idx_path)) {
            LOG_INFO("Loaded existing index from %s", idx_path.c_str());
            indexed_.store(true);
            return true;
        }
    }

    LOG_INFO("Starting parallel scan of %zu stripe(s)...", bak_paths_.size());
    auto start_time = std::chrono::steady_clock::now();

    // First, parse header from first stripe to get data start offset
    {
        BackupStream stream(bak_paths_[0]);
        BackupHeaderParser parser(stream);
        if (!parser.parse()) {
            LOG_ERROR("Failed to parse backup header");
            return false;
        }

        data_start_offset_ = parser.data_start_offset();
        is_compressed_ = !parser.backup_sets().empty() && 
                         parser.backup_sets()[0].is_compressed;

        LOG_INFO("Backup metadata: data_offset=%llu, compressed=%d",
                 (unsigned long long)data_start_offset_, is_compressed_ ? 1 : 0);
    }

    if (is_compressed_) {
        decompressor_ = std::make_unique<Decompressor>();
    }

    // Launch parallel scan threads
    size_t stripes_per_thread = (bak_paths_.size() + config_.num_threads - 1) / config_.num_threads;
    
    // For striped backups, one thread per stripe is most efficient
    size_t actual_threads = std::min(config_.num_threads, bak_paths_.size());
    
    std::atomic<int> completed_stripes{0};
    std::vector<std::thread> threads;
    threads.reserve(actual_threads);

    for (size_t t = 0; t < actual_threads; ++t) {
        threads.emplace_back([this, t, actual_threads, &progress, &completed_stripes]() {
            for (size_t s = t; s < bak_paths_.size(); s += actual_threads) {
                scan_stripe(static_cast<int>(s), progress);
                ++completed_stripes;
            }
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    LOG_INFO("Scan complete: %zu pages indexed, %llu bytes read in %lld ms",
             index_.size(),
             (unsigned long long)bytes_read_.load(),
             (long long)duration_ms);

    LOG_INFO("Index memory usage: %.2f MB",
             index_.memory_usage_bytes() / (1024.0 * 1024.0));

    // Save index for future use
    if (config_.save_index && index_.size() > 0) {
        std::string idx_path = index_file_path();
        index_.save_to_file(idx_path);
    }

    indexed_.store(true);
    return true;
}

void IndexedPageStore::scan_stripe(int stripe_index, ScanProgressCallback progress) {
    const std::string& path = bak_paths_[stripe_index];
    LOG_INFO("Scanning stripe %d: %s", stripe_index, path.c_str());

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        LOG_ERROR("Failed to open stripe %d: %s", stripe_index, path.c_str());
        return;
    }

    // Get file size
    file.seekg(0, std::ios::end);
    uint64_t file_size = file.tellg();

    // Start after MTF header (use data_start_offset from first stripe as approximation)
    uint64_t offset = data_start_offset_;
    offset = (offset + PAGE_SIZE - 1) & ~(static_cast<uint64_t>(PAGE_SIZE) - 1);
    if (offset == 0) offset = PAGE_SIZE;

    file.seekg(static_cast<std::streamoff>(offset));

    // Read buffer for chunk scanning
    const size_t chunk_size = config_.scan_chunk_size;
    const size_t pages_per_chunk = chunk_size / PAGE_SIZE;
    std::vector<uint8_t> chunk_buffer(chunk_size);
    std::vector<uint8_t> decomp_buffer;
    
    if (is_compressed_) {
        decomp_buffer.resize(chunk_size * 4);  // Decompressed can be larger
    }

    uint64_t stripe_pages = 0;
    uint64_t stripe_bytes = 0;

    while (offset < file_size) {
        // Read chunk
        size_t to_read = std::min(chunk_size, static_cast<size_t>(file_size - offset));
        file.read(reinterpret_cast<char*>(chunk_buffer.data()), static_cast<std::streamsize>(to_read));
        size_t bytes_read = static_cast<size_t>(file.gcount());

        if (bytes_read == 0) break;

        const uint8_t* page_data = chunk_buffer.data();
        size_t available = bytes_read;

        // Handle decompression if needed
        if (is_compressed_ && decompressor_) {
            size_t decomp_size = decompressor_->decompress_into(
                chunk_buffer.data(), bytes_read,
                decomp_buffer.data(), decomp_buffer.size());
            if (decomp_size > 0) {
                page_data = decomp_buffer.data();
                available = decomp_size;
            }
        }

        // Process pages in chunk
        size_t page_offset = 0;
        while (page_offset + PAGE_SIZE <= available) {
            const uint8_t* page_ptr = page_data + page_offset;

            // Validate page header
            const auto* hdr = reinterpret_cast<const PageHeader*>(page_ptr);
            
            // Basic validity checks - page has valid file/page IDs
            if (hdr->this_page != 0 || hdr->this_file != 0) {
                // Looks like a valid page
                uint32_t obj_id = 0;
                IndexedPageType page_type = classify_page(page_ptr, obj_id);

                PageIndexEntry entry{};
                entry.stripe_index = static_cast<uint8_t>(stripe_index);
                entry.page_type = static_cast<uint8_t>(page_type);
                entry.object_id = obj_id;
                entry.file_offset = offset + page_offset;

                index_.add_entry(static_cast<int32_t>(hdr->this_file), 
                                static_cast<int32_t>(hdr->this_page), entry);
                ++stripe_pages;
            }

            page_offset += PAGE_SIZE;
        }

        stripe_bytes += bytes_read;
        offset += bytes_read;
        pages_scanned_.fetch_add(pages_per_chunk);
        bytes_read_.fetch_add(bytes_read);

        if (progress) {
            progress(pages_scanned_.load(), bytes_read_.load(), stripe_index);
        }
    }

    LOG_INFO("Stripe %d: %llu pages, %llu bytes",
             stripe_index, (unsigned long long)stripe_pages,
             (unsigned long long)stripe_bytes);
}

IndexedPageType IndexedPageStore::classify_page(const uint8_t* page_data, uint32_t& out_object_id) {
    const auto* hdr = reinterpret_cast<const PageHeader*>(page_data);
    out_object_id = hdr->obj_id;

    // Page type is in type field
    switch (hdr->type) {
        case 1:  return IndexedPageType::Data;
        case 2:  return IndexedPageType::Index;
        case 3:  return IndexedPageType::TextMix;
        case 4:  return IndexedPageType::TextTree;
        case 8:  return IndexedPageType::GAM;
        case 9:  return IndexedPageType::SGAM;
        case 10: return IndexedPageType::IAM;
        case 11: return IndexedPageType::PFS;
        case 13: return IndexedPageType::Boot;
        case 15: return IndexedPageType::FileHeader;
        default:
            // System tables have object_id < 100 typically
            if (out_object_id > 0 && out_object_id < 100) {
                return IndexedPageType::System;
            }
            return IndexedPageType::Unknown;
    }
}

bool IndexedPageStore::get_page(int32_t file_id, int32_t page_id, uint8_t* out_buffer) {
    // Ensure index is built
    if (!indexed_.load()) {
        if (!scan(nullptr)) {
            return false;
        }
    }

    int64_t key = make_page_key(file_id, page_id);

    // Check cache first
    if (cache_.get(key, out_buffer)) {
        return true;
    }

    // Lookup in index
    PageIndexEntry entry;
    if (!index_.lookup(file_id, page_id, entry)) {
        return false;
    }

    // Read from stripe file
    if (!read_page_from_stripe(entry, out_buffer)) {
        return false;
    }

    // Add to cache
    cache_.put(key, out_buffer);
    return true;
}

bool IndexedPageStore::read_page_from_stripe(const PageIndexEntry& entry, uint8_t* out_buffer) {
    int stripe_idx = entry.stripe_index;
    
    if (stripe_idx < 0 || stripe_idx >= static_cast<int>(bak_paths_.size())) {
        LOG_ERROR("Invalid stripe index: %d", stripe_idx);
        return false;
    }

    std::lock_guard<std::mutex> lock(stripe_mutexes_[stripe_idx]);

    // Open file if not already open
    if (!stripe_files_[stripe_idx]) {
        stripe_files_[stripe_idx] = std::make_unique<std::ifstream>(
            bak_paths_[stripe_idx], std::ios::binary);
        
        if (!stripe_files_[stripe_idx]->is_open()) {
            LOG_ERROR("Failed to open stripe for reading: %s", bak_paths_[stripe_idx].c_str());
            return false;
        }
    }

    auto& file = *stripe_files_[stripe_idx];

    // Seek to page offset
    file.seekg(static_cast<std::streamoff>(entry.file_offset));
    if (!file) {
        LOG_ERROR("Failed to seek to offset %llu in stripe %d",
                  (unsigned long long)entry.file_offset, stripe_idx);
        return false;
    }

    // Read page
    file.read(reinterpret_cast<char*>(out_buffer), PAGE_SIZE);
    if (file.gcount() != PAGE_SIZE) {
        LOG_ERROR("Short read at offset %llu: got %lld bytes",
                  (unsigned long long)entry.file_offset, (long long)file.gcount());
        return false;
    }

    // Handle decompression if needed
    if (is_compressed_ && decompressor_) {
        std::vector<uint8_t> decomp_buf(PAGE_SIZE * 2);
        size_t decomp_size = decompressor_->decompress_into(
            out_buffer, PAGE_SIZE, decomp_buf.data(), decomp_buf.size());
        if (decomp_size >= PAGE_SIZE) {
            std::memcpy(out_buffer, decomp_buf.data(), PAGE_SIZE);
        }
    }

    return true;
}

}  // namespace bakread
