#include "bakread/direct_extractor.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cstring>

namespace bakread {

DirectExtractor::DirectExtractor(const std::vector<std::string>& bak_paths,
                                 const DirectExtractorConfig& config)
    : bak_paths_(bak_paths)
    , config_(config)
{
    if (config_.use_indexed_mode) {
        LOG_INFO("Using indexed page store mode (cache: %zu MB)", config_.cache_size_mb);
        
        IndexedStoreConfig store_config;
        store_config.cache_pages = (config_.cache_size_mb * 1024 * 1024) / 8192;  // MB to pages
        store_config.index_dir = config_.index_dir;
        store_config.force_rescan = config_.force_rescan;
        
        indexed_store_ = std::make_unique<IndexedPageStore>(bak_paths_, store_config);
    }
}

DirectExtractor::~DirectExtractor() = default;

void DirectExtractor::set_table(const std::string& schema, const std::string& table) {
    target_schema_ = schema;
    target_table_  = table;
}

void DirectExtractor::set_columns(const std::vector<std::string>& columns) {
    target_columns_ = columns;
}

void DirectExtractor::set_max_rows(int64_t max_rows) {
    max_rows_ = max_rows;
}

void DirectExtractor::set_allocation_hints(std::unordered_set<int64_t> hints) {
    allocation_hints_ = std::move(hints);
}

void DirectExtractor::set_progress_callback(ProgressCallback cb) {
    progress_cb_ = cb;
}

const BackupInfo& DirectExtractor::backup_info() const {
    static BackupInfo empty;
    return header_parser_ ? header_parser_->info() : empty;
}

ListTablesResult DirectExtractor::list_tables() {
    ListTablesResult result;

    try {
        // Phase 1: Headers
        LOG_INFO("=== Direct Extract Mode - List Tables ===");
        if (!phase_parse_headers()) {
            result.error_message = "Failed to parse backup headers";
            return result;
        }

        // Check for TDE / encryption (warn but continue)
        if (header_parser_->is_tde_enabled()) {
            result.error_message = "TDE detected. Cannot list tables directly.";
            LOG_WARN("%s", result.error_message.c_str());
            return result;
        }

        if (header_parser_->is_backup_encrypted()) {
            result.error_message = "Backup encryption detected. Cannot list tables directly.";
            LOG_WARN("%s", result.error_message.c_str());
            return result;
        }

        // Phase 2: Load pages
        if (!phase_load_pages()) {
            result.error_message = "Failed to read pages from backup stream";
            return result;
        }

        // Phase 3: Build catalog
        LOG_INFO("Building system catalog...");
        catalog_ = std::make_unique<CatalogReader>(
            [this](int32_t file_id, int32_t page_id, uint8_t* buf) {
                return this->provide_page(file_id, page_id, buf);
            });

        if (!catalog_->scan_catalog()) {
            result.error_message = "Failed to scan system catalog";
            return result;
        }

        // Get all user tables
        auto user_tables = catalog_->list_user_tables();
        LOG_INFO("Found %zu user tables in catalog", user_tables.size());

        for (const auto& obj : user_tables) {
            TableInfo info;
            info.object_id = obj.object_id;
            info.schema_name = obj.name.empty() ? "dbo" : ""; // Will need schema lookup
            info.table_name = obj.name;
            
            // Get schema name
            // The SystemObject has schema_id, we need to look it up
            std::string schema = "dbo";  // default
            // This is simplified - the actual schema name resolution is in catalog
            info.full_name = schema + "." + obj.name;
            info.schema_name = schema;
            
            // Try to get row/page estimates from allocation units
            auto allocs = catalog_->get_allocation_units(obj.object_id);
            if (!allocs.empty()) {
                info.page_count = 0;
                for (const auto& au : allocs) {
                    if (au.type == 1) {  // IN_ROW_DATA
                        // Estimate based on first/root page info
                    }
                }
            }

            result.tables.push_back(std::move(info));
        }

        result.success = !result.tables.empty();
        if (!result.success && result.error_message.empty()) {
            result.error_message = "No user tables found in catalog";
        }

    } catch (const BakReadError& e) {
        result.error_message = e.what();
        LOG_ERROR("List tables failed: %s", e.what());
    } catch (const std::exception& e) {
        result.error_message = std::string("Unexpected error: ") + e.what();
        LOG_ERROR("List tables failed: %s", e.what());
    }

    return result;
}

DirectExtractResult DirectExtractor::extract(RowCallback row_callback) {
    DirectExtractResult result;

    try {
        // Phase 1: Headers
        LOG_INFO("=== Direct Extract Mode (Mode A) ===");
        if (!phase_parse_headers()) {
            result.error_message = "Failed to parse backup headers";
            return result;
        }

        // Check for TDE / encryption
        if (header_parser_->is_tde_enabled()) {
            result.tde_detected = true;
            result.error_message =
                "TDE detected. Direct parsing not supported. "
                "Provide certificate or use restore mode (--mode restore).";
            LOG_ERROR("%s", result.error_message.c_str());
            return result;
        }

        if (header_parser_->is_backup_encrypted()) {
            result.encryption_detected = true;
            result.error_message =
                "Backup encryption detected. Direct parsing not supported. "
                "Use restore mode with appropriate certificates.";
            LOG_ERROR("%s", result.error_message.c_str());
            return result;
        }

        // Phase 2: Load pages
        if (!phase_load_pages()) {
            result.error_message = "Failed to read pages from backup stream";
            return result;
        }

        // Phase 3: Resolve table
        if (!phase_resolve_table()) {
            result.error_message = "Failed to resolve table '" +
                target_schema_ + "." + target_table_ +
                "' from system catalog";
            return result;
        }

        // Phase 4: Extract rows
        result.rows_read = phase_extract_rows(row_callback);
        result.success = true;

        LOG_INFO("Direct extraction complete: %llu rows",
                 (unsigned long long)result.rows_read);

    } catch (const BakReadError& e) {
        result.error_message = e.what();
        LOG_ERROR("Direct extraction failed: %s", e.what());
    } catch (const std::exception& e) {
        result.error_message = std::string("Unexpected error: ") + e.what();
        LOG_ERROR("Direct extraction failed: %s", e.what());
    }

    return result;
}

bool DirectExtractor::phase_parse_headers() {
    LOG_INFO("Phase 1: Parsing backup headers from %zu file(s)...",
             bak_paths_.size());

    stream_ = std::make_unique<BackupStream>(bak_paths_[0]);
    header_parser_ = std::make_unique<BackupHeaderParser>(*stream_);
    decompressor_ = std::make_unique<Decompressor>();

    return header_parser_->parse();
}

bool DirectExtractor::phase_load_pages() {
    // In indexed mode, the IndexedPageStore handles scanning
    if (indexed_store_) {
        LOG_INFO("Phase 2: Building page index (indexed mode)...");
        
        auto progress = [this](uint64_t pages, uint64_t bytes, int stripe) {
            if (progress_cb_) {
                Progress p;
                p.bytes_processed = bytes;
                p.bytes_total = 0;  // Unknown during scan
                p.rows_exported = pages;
                p.pct = 0.0;
                progress_cb_(p);
            }
            if (pages % 100000 == 0) {
                LOG_INFO("Indexed %llu pages, %.2f GB scanned (stripe %d)",
                         (unsigned long long)pages, bytes / 1e9, stripe);
            }
        };
        
        bool ok = indexed_store_->scan(progress);
        if (ok) {
            LOG_INFO("Index built: %zu pages, cache hit rate: %.1f%%",
                     indexed_store_->index().size(),
                     indexed_store_->cache_hit_rate() * 100.0);
        }
        return ok;
    }

    LOG_INFO("Phase 2: Reading pages from %zu backup file(s)...",
             bak_paths_.size());

    if (!allocation_hints_.empty()) {
        LOG_INFO("Allocation hint active: filtering to %zu target pages",
                 allocation_hints_.size());
    }

    uint64_t scan_start = header_parser_->data_start_offset();
    scan_start = (scan_start + PAGE_SIZE - 1) & ~(static_cast<uint64_t>(PAGE_SIZE) - 1);
    if (scan_start == 0) scan_start = PAGE_SIZE;

    constexpr size_t CHUNK_PAGES = 128;
    constexpr size_t CHUNK_SIZE  = PAGE_SIZE * CHUNK_PAGES;
    std::vector<uint8_t> buf(CHUNK_SIZE);

    uint64_t total_pages_found = 0;
    uint64_t total_bytes = 0;
    for (auto& p : bak_paths_) {
        BackupStream probe(p);
        total_bytes += probe.file_size();
    }

    for (size_t fi = 0; fi < bak_paths_.size(); ++fi) {
        auto& path = bak_paths_[fi];
        LOG_INFO("Scanning stripe %zu/%zu: %s",
                 fi + 1, bak_paths_.size(), path.c_str());

        auto stripe_stream = (fi == 0)
            ? std::move(stream_)
            : std::make_unique<BackupStream>(path);

        uint64_t stripe_size = stripe_stream->file_size();
        uint64_t pages_found = 0;

        stripe_stream->seek(scan_start);

        while (!stripe_stream->eof()) {
            uint64_t chunk_file_off = stripe_stream->position();
            size_t got = stripe_stream->read(buf.data(), buf.size());
            if (got < PAGE_SIZE) break;

            for (size_t off = 0; off + PAGE_SIZE <= got; off += PAGE_SIZE) {
                const uint8_t* page = buf.data() + off;
                PageHeader hdr;
                std::memcpy(&hdr, page, sizeof(hdr));

                if (hdr.header_version != 1) continue;
                if (hdr.type < 1 || hdr.type > 17) continue;
                if (hdr.this_file < 1 || hdr.this_file > 32) continue;
                if (hdr.slot_count > 1000) continue;
                if (hdr.free_count > PAGE_SIZE) continue;

                cache_page(hdr.this_file, hdr.this_page, page);
                ++pages_found;
            }

            if (progress_cb_) {
                uint64_t pos = chunk_file_off + got;
                if (pos % (16 * 1024 * 1024) < CHUNK_SIZE) {
                    Progress p;
                    p.bytes_processed = pos;
                    p.bytes_total     = stripe_size;
                    p.pct             = stripe_stream->progress_pct();
                    progress_cb_(p);
                }
            }

            if (page_cache_.size() * PAGE_SIZE > 512ULL * 1024 * 1024) {
                LOG_WARN("Page cache limit reached (%zu pages, ~%zu MB).",
                         page_cache_.size(),
                         page_cache_.size() * PAGE_SIZE / (1024 * 1024));
                break;
            }
        }

        if (pages_found == 0) {
            LOG_WARN("No pages at 8KB boundaries in stripe %zu; trying 512-byte scan...",
                     fi + 1);
            stripe_stream->seek(scan_start);

            while (!stripe_stream->eof()) {
                size_t got = stripe_stream->read(buf.data(), buf.size());
                if (got < PAGE_SIZE) break;

                for (size_t off = 0; off + PAGE_SIZE <= got; off += 512) {
                    const uint8_t* page = buf.data() + off;
                    PageHeader hdr;
                    std::memcpy(&hdr, page, sizeof(hdr));

                    if (hdr.header_version != 1) continue;
                    if (hdr.type < 1 || hdr.type > 17) continue;
                    if (hdr.this_file < 1 || hdr.this_file > 32) continue;
                    if (hdr.slot_count > 1000) continue;
                    if (hdr.free_count > PAGE_SIZE) continue;

                    cache_page(hdr.this_file, hdr.this_page, page);
                    ++pages_found;
                }

                if (page_cache_.size() * PAGE_SIZE > 512ULL * 1024 * 1024) break;
            }
        }

        LOG_INFO("Stripe %zu: %llu pages found", fi + 1,
                 (unsigned long long)pages_found);
        total_pages_found += pages_found;

        if (fi == 0) {
            stream_ = std::move(stripe_stream);
        }
    }

    LOG_INFO("Page scan complete: %llu pages cached (%zu MB) from %zu file(s)",
             (unsigned long long)total_pages_found,
             page_cache_.size() * PAGE_SIZE / (1024 * 1024),
             bak_paths_.size());

    return total_pages_found > 0;
}

bool DirectExtractor::phase_resolve_table() {
    LOG_INFO("Phase 3: Resolving table '%s.%s' from system catalog...",
             target_schema_.c_str(), target_table_.c_str());

    auto provider = [this](int32_t fid, int32_t pid, uint8_t* buf) -> bool {
        return this->provide_page(fid, pid, buf);
    };

    catalog_ = std::make_unique<CatalogReader>(provider);

    if (!catalog_->scan_catalog()) {
        LOG_ERROR("System catalog scan failed");
        return false;
    }

    // List discovered tables
    auto tables = catalog_->list_user_tables();
    if (!tables.empty()) {
        LOG_INFO("Discovered user tables:");
        for (auto& t : tables) {
            LOG_INFO("  %s (object_id=%d)", t.name.c_str(), t.object_id);
        }
    }

    if (!catalog_->resolve_table(target_schema_, target_table_, schema_)) {
        LOG_ERROR("Table '%s.%s' not found in catalog",
                  target_schema_.c_str(), target_table_.c_str());

        if (!tables.empty()) {
            LOG_INFO("Available tables:");
            for (auto& t : tables)
                LOG_INFO("  %s", t.name.c_str());
        }
        return false;
    }

    // Apply column filter if specified
    if (!target_columns_.empty()) {
        std::vector<ColumnDef> filtered;
        for (auto& req_col : target_columns_) {
            bool found = false;
            for (auto& col : schema_.columns) {
                if (col.name == req_col) {
                    filtered.push_back(col);
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOG_WARN("Requested column '%s' not found in table schema",
                         req_col.c_str());
            }
        }
        if (!filtered.empty()) {
            schema_.columns = std::move(filtered);
        }
    }

    LOG_INFO("Table schema resolved: %zu columns", schema_.columns.size());
    for (auto& col : schema_.columns) {
        LOG_DEBUG("  Column: %s (type=%d, len=%d, nullable=%d)",
                  col.name.c_str(), static_cast<int>(col.type),
                  col.max_length, col.is_nullable);
    }

    return true;
}

uint64_t DirectExtractor::phase_extract_rows(RowCallback& callback) {
    LOG_INFO("Phase 4: Extracting rows...");

    RowDecoder decoder(schema_);
    uint64_t total_rows = 0;

    uint32_t target_page_objid = catalog_->get_page_obj_id(schema_.object_id);
    LOG_INFO("Table %s (object_id=%d) maps to page header obj_id=%u",
             schema_.qualified_name().c_str(), schema_.object_id, target_page_objid);

    if (target_page_objid == 0) {
        LOG_ERROR("Could not resolve page header obj_id for table %s",
                  schema_.qualified_name().c_str());
        return 0;
    }

    std::vector<int64_t> candidate_pages;

    for (auto& [key, page_data] : page_cache_) {
        PageHeader hdr;
        std::memcpy(&hdr, page_data.data(), sizeof(hdr));

        if (hdr.type != static_cast<uint8_t>(PageType::Data)) continue;
        if (hdr.slot_count == 0) continue;
        if (hdr.obj_id != target_page_objid) continue;

        // If allocation hints are provided, only include pages in the hint set
        if (!allocation_hints_.empty() &&
            allocation_hints_.find(key) == allocation_hints_.end()) {
            continue;
        }

        candidate_pages.push_back(key);
    }

    if (!allocation_hints_.empty()) {
        LOG_INFO("Allocation hint filtered to %zu pages (from %zu hints)",
                 candidate_pages.size(), allocation_hints_.size());
    }
    LOG_INFO("Scanning %zu candidate data pages...", candidate_pages.size());

    for (auto key : candidate_pages) {
        auto& page_data = page_cache_[key];
        std::vector<Row> rows;

        int decoded = decoder.decode_page(page_data.data(), rows);
        if (decoded <= 0) continue;

        for (auto& row : rows) {
            if (max_rows_ >= 0 && total_rows >= static_cast<uint64_t>(max_rows_))
                break;

            if (!callback(row)) break;
            ++total_rows;
        }

        if (max_rows_ >= 0 && total_rows >= static_cast<uint64_t>(max_rows_))
            break;

        // Progress reporting
        if (progress_cb_ && total_rows % 10000 == 0) {
            Progress p;
            p.rows_exported = total_rows;
            progress_cb_(p);
        }
    }

    return total_rows;
}

bool DirectExtractor::provide_page(int32_t file_id, int32_t page_id, uint8_t* buf) {
    // Use indexed store if available
    if (indexed_store_) {
        return indexed_store_->get_page(file_id, page_id, buf);
    }

    // Fall back to in-memory cache
    auto it = page_cache_.find(page_key(file_id, page_id));
    if (it == page_cache_.end()) return false;

    std::memcpy(buf, it->second.data(), PAGE_SIZE);
    return true;
}

void DirectExtractor::cache_page(int32_t file_id, int32_t page_id,
                                  const uint8_t* data) {
    // In indexed mode, pages are managed by IndexedPageStore
    if (indexed_store_) return;

    auto key = page_key(file_id, page_id);
    if (page_cache_.find(key) != page_cache_.end()) return;

    page_cache_[key].assign(data, data + PAGE_SIZE);
}

std::vector<SystemModule> DirectExtractor::list_modules() {
    // Ensure catalog is built
    if (!catalog_) {
        ListTablesResult dummy = list_tables();
        if (!dummy.success || !catalog_) {
            return {};
        }
    }
    return catalog_->list_modules();
}

std::vector<SystemPrincipal> DirectExtractor::list_principals() {
    if (!catalog_) {
        ListTablesResult dummy = list_tables();
        if (!dummy.success || !catalog_) {
            return {};
        }
    }
    return catalog_->list_principals();
}

std::vector<SystemRoleMember> DirectExtractor::list_role_members() {
    if (!catalog_) {
        ListTablesResult dummy = list_tables();
        if (!dummy.success || !catalog_) {
            return {};
        }
    }
    return catalog_->list_role_members();
}

std::vector<SystemPermission> DirectExtractor::list_permissions() {
    if (!catalog_) {
        ListTablesResult dummy = list_tables();
        if (!dummy.success || !catalog_) {
            return {};
        }
    }
    return catalog_->list_permissions();
}

}  // namespace bakread
