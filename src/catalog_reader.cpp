#include "bakread/catalog_reader.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cstring>

namespace bakread {

// Well-known object IDs for system base tables
static constexpr int32_t OBJ_SYSSCHOBJS    = 34;   // sys.sysschobjs
static constexpr int32_t OBJ_SYSCOLPARS    = 41;   // sys.syscolpars
static constexpr int32_t OBJ_SYSIDXSTATS  = 54;   // sys.sysidxstats
static constexpr int32_t OBJ_SYSALLOCUNITS = 7;    // sys.sysallocunits
static constexpr int32_t OBJ_SYSROWSETS    = 5;    // sys.sysrowsets
static constexpr int32_t OBJ_SYSOWNERS     = 27;   // sys.sysowners (schemas)

// The boot page is always page 9 of file 1
static constexpr int32_t BOOT_PAGE_ID = 9;
static constexpr int32_t PRIMARY_FILE_ID = 1;

CatalogReader::CatalogReader(PageProvider provider)
    : page_provider_(std::move(provider))
{
    // Initialize well-known schema names
    schema_names_[1] = "dbo";
    schema_names_[2] = "guest";
    schema_names_[3] = "INFORMATION_SCHEMA";
    schema_names_[4] = "sys";
}

bool CatalogReader::scan_catalog() {
    LOG_INFO("Scanning system catalog...");

    if (!read_boot_page()) {
        LOG_ERROR("Failed to read boot page -- catalog scan aborted");
        return false;
    }

    // Scan system tables in dependency order
    scan_system_objects();
    scan_system_columns();
    scan_system_indexes();
    scan_system_allocation_units();
    scan_rowset_allocunit_mapping();

    LOG_INFO("Catalog scan complete: %zu objects, %zu column sets",
             objects_.size(), columns_.size());
    return true;
}

bool CatalogReader::read_boot_page() {
    uint8_t page[PAGE_SIZE];
    if (!page_provider_(PRIMARY_FILE_ID, BOOT_PAGE_ID, page)) {
        LOG_WARN("Cannot read boot page (1:%d)", BOOT_PAGE_ID);
        return false;
    }

    PageHeader hdr;
    std::memcpy(&hdr, page, sizeof(hdr));

    if (hdr.type != static_cast<uint8_t>(PageType::Boot)) {
        LOG_WARN("Page 1:%d is not a boot page (type=%d)", BOOT_PAGE_ID, hdr.type);
        return false;
    }

    LOG_DEBUG("Boot page found: version=%d, slots=%d", hdr.header_version, hdr.slot_count);

    // The boot page contains database metadata and system table root pages.
    // The exact layout varies by SQL Server version, but key system table
    // page references are at known offsets within the boot page data area.
    //
    // We scan the boot page data for recognizable system table root page IDs
    // by looking for small page numbers that correspond to the early pages
    // of file 1 where system tables typically reside.
    //
    // A more precise approach would parse the exact boot page structure for
    // each SQL Server version, which is a Phase 2 enhancement.

    // For now, we'll use a heuristic approach: scan pages 1-200 of file 1
    // looking for data pages with system object IDs in their metadata.
    sysschobjs_root_page_  = 0;
    syscolpars_root_page_  = 0;
    sysidxstats_root_page_ = 0;

    return true;
}

bool CatalogReader::scan_system_objects() {
    LOG_DEBUG("Scanning for system objects (sysschobjs)...");

    uint8_t page[PAGE_SIZE];
    int found_count = 0;

    // Scan early pages of file 1 looking for data pages that contain
    // system object definitions. System tables are typically on pages 1-300.
    for (int32_t pg = 1; pg < 1000; ++pg) {
        if (!page_provider_(PRIMARY_FILE_ID, pg, page)) continue;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));

        if (hdr.type != static_cast<uint8_t>(PageType::Data)) continue;
        if (hdr.slot_count == 0) continue;
        if (hdr.obj_id != OBJ_SYSSCHOBJS) continue;

        for (int slot = 0; slot < hdr.slot_count; ++slot) {
            uint16_t offset = get_slot_offset(page, slot);
            if (offset < PAGE_HEADER_SIZE || offset >= PAGE_SIZE - 10) continue;

            const uint8_t* rec = page + offset;
            uint8_t status = rec[0];
            if ((status & RecordStatus::TypeMask) != RecordStatus::PrimaryRecord)
                continue;

            uint16_t fixed_end;
            std::memcpy(&fixed_end, rec + 2, 2);
            if (fixed_end < 20 || fixed_end > PAGE_SIZE) continue;

            // sysschobjs fixed region layout (approximate):
            //   offset 4: object_id (4 bytes)
            //   offset 8: schema_id (4 bytes)
            //   offset 12: type (2 bytes, char)
            //   Variable region: name (nvarchar)
            //
            // We try to identify rows by checking if the object_id and
            // schema_id look plausible, and if there's a readable name
            // in the variable region.

            if (fixed_end < 14) continue;

            int32_t obj_id, schema_id;
            std::memcpy(&obj_id, rec + 4, 4);
            std::memcpy(&schema_id, rec + 8, 4);

            // Plausibility check: positive IDs, schema_id in reasonable range
            if (obj_id <= 0 || schema_id <= 0 || schema_id > 65536) continue;

            // Check if there's variable data with a name
            bool has_var = (status & RecordStatus::HasVarColumns) != 0;
            if (!has_var) continue;

            // The null bitmap area starts with a 2-byte column count,
            // followed by ceil(column_count/8) actual bitmap bytes.
            if (fixed_end + 2 >= PAGE_SIZE - offset) continue;
            uint16_t col_count;
            std::memcpy(&col_count, rec + fixed_end, 2);
            if (col_count == 0 || col_count > 256) continue;

            int null_bmp_bytes = (col_count + 7) / 8;
            size_t var_area_start = fixed_end + 2 + null_bmp_bytes;

            if (var_area_start + 2 >= PAGE_SIZE - offset) continue;

            uint16_t var_count;
            std::memcpy(&var_count, rec + var_area_start, 2);
            if (var_count == 0 || var_count > 20) continue;

            size_t offsets_end = var_area_start + 2 + var_count * 2;
            if (offsets_end >= PAGE_SIZE - offset) continue;

            uint16_t first_var_end;
            std::memcpy(&first_var_end, rec + var_area_start + 2, 2);
            first_var_end &= 0x7FFF;

            size_t name_start = offsets_end;
            size_t name_len = 0;
            if (first_var_end > name_start && first_var_end < PAGE_SIZE - offset) {
                name_len = first_var_end - name_start;
            }

            if (name_len < 2 || name_len > 256) continue;

            // Try to read as UTF-16LE
            std::string name;
            for (size_t ni = 0; ni + 1 < name_len; ni += 2) {
                uint16_t ch = rec[name_start + ni] |
                              (static_cast<uint16_t>(rec[name_start + ni + 1]) << 8);
                if (ch == 0) break;
                if (ch >= 0x20 && ch < 0x7F)
                    name.push_back(static_cast<char>(ch));
                else if (ch >= 0x80)
                    name.push_back('?');
                else
                    break;
            }

            if (name.empty()) continue;

            // Extract type code (2-char code like "U ", "S ", etc.)
            // In sysschobjs: id(4) @ 4, nsid(4) @ 8, nsclass(1) @ 12,
            // status(4) @ 13, type(char(2)) @ 17
            char type_code[3] = {};
            if (fixed_end > 18) {
                type_code[0] = static_cast<char>(rec[17]);
                type_code[1] = static_cast<char>(rec[18]);
            }

            SystemObject obj;
            obj.object_id = obj_id;
            obj.schema_id = schema_id;
            obj.name      = name;
            obj.type      = type_code;
            objects_[obj_id] = obj;
            ++found_count;

            LOG_DEBUG("  Found object: id=%d schema=%d type='%s' name='%s'",
                      obj_id, schema_id, type_code, name.c_str());
        }
    }

    LOG_INFO("Found %d system objects via page scan", found_count);
    return found_count > 0;
}

bool CatalogReader::scan_system_columns() {
    LOG_DEBUG("Scanning for system columns (syscolpars)...");

    uint8_t page[PAGE_SIZE];
    int found_count = 0;

    for (int32_t pg = 1; pg < 1000; ++pg) {
        if (!page_provider_(PRIMARY_FILE_ID, pg, page)) continue;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));

        if (hdr.type != static_cast<uint8_t>(PageType::Data)) continue;
        if (hdr.slot_count == 0) continue;
        if (hdr.obj_id != OBJ_SYSCOLPARS) continue;

        for (int slot = 0; slot < hdr.slot_count; ++slot) {
            uint16_t offset = get_slot_offset(page, slot);
            if (offset < PAGE_HEADER_SIZE || offset >= PAGE_SIZE - 10) continue;

            const uint8_t* rec = page + offset;
            uint8_t status = rec[0];
            if ((status & RecordStatus::TypeMask) != RecordStatus::PrimaryRecord)
                continue;

            uint16_t fixed_end;
            std::memcpy(&fixed_end, rec + 2, 2);

            // syscolpars fixed layout (SQL Server 2016+):
            //   offset 4:  id (int32) - owning object_id
            //   offset 8:  number (int16) - procedure param number
            //   offset 10: colid (int32) - column_id
            //   offset 14: xtype (tinyint) - system type id
            //   offset 15: utype (int32) - user type
            //   offset 19: length (int16) - max_length
            //   offset 21: prec (tinyint) - precision
            //   offset 22: scale (tinyint) - scale

            if (fixed_end < 23) continue;

            int32_t obj_id;
            std::memcpy(&obj_id, rec + 4, 4);

            if (objects_.find(obj_id) == objects_.end()) continue;

            int32_t col_id;
            std::memcpy(&col_id, rec + 10, 4);

            if (col_id <= 0 || col_id > 4096) continue;

            SystemColumn col;
            col.object_id      = obj_id;
            col.column_id      = col_id;
            col.system_type_id = rec[14];
            std::memcpy(&col.max_length, rec + 19, 2);
            col.precision      = rec[21];
            col.scale          = rec[22];

            // Extract column name from variable region.
            // The null bitmap starts with a 2-byte column count.
            bool has_var = (status & RecordStatus::HasVarColumns) != 0;
            if (has_var && fixed_end + 2 < PAGE_SIZE - offset) {
                uint16_t col_count;
                std::memcpy(&col_count, rec + fixed_end, 2);
                if (col_count > 0 && col_count <= 256) {
                    int null_bmp = (col_count + 7) / 8;
                    size_t va = fixed_end + 2 + null_bmp;

                    if (va + 2 < PAGE_SIZE - offset) {
                        uint16_t vc;
                        std::memcpy(&vc, rec + va, 2);
                        if (vc > 0 && vc <= 20) {
                            size_t oe = va + 2 + vc * 2;
                            if (oe < PAGE_SIZE - offset) {
                                uint16_t fve;
                                std::memcpy(&fve, rec + va + 2, 2);
                                fve &= 0x7FFF;
                                if (fve > oe && fve < PAGE_SIZE - offset) {
                                    size_t nlen = fve - oe;
                                    if (nlen >= 2 && nlen <= 256) {
                                        for (size_t ni = 0; ni + 1 < nlen; ni += 2) {
                                            uint16_t ch = rec[oe+ni] |
                                                (static_cast<uint16_t>(rec[oe+ni+1]) << 8);
                                            if (ch == 0) break;
                                            if (ch >= 0x20 && ch < 0x7F)
                                                col.name.push_back(static_cast<char>(ch));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            columns_[obj_id].push_back(col);
            ++found_count;
        }
    }

    // Sort columns by column_id for each object
    for (auto& [oid, cols] : columns_) {
        std::sort(cols.begin(), cols.end(),
                  [](const SystemColumn& a, const SystemColumn& b) {
                      return a.column_id < b.column_id;
                  });
    }

    LOG_INFO("Found %d column definitions via page scan", found_count);
    return found_count > 0;
}

bool CatalogReader::scan_system_indexes() {
    LOG_DEBUG("Scanning for indexes (sysidxstats)...");
    // Simplified: mark everything as heap unless we find clustered index evidence
    return true;
}

bool CatalogReader::scan_system_allocation_units() {
    LOG_DEBUG("Scanning for allocation units...");

    uint8_t page[PAGE_SIZE];

    // Scan IAM pages to build allocation information
    for (int32_t pg = 1; pg < 1000; ++pg) {
        if (!page_provider_(PRIMARY_FILE_ID, pg, page)) continue;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));

        if (hdr.type == static_cast<uint8_t>(PageType::IAM)) {
            SystemAllocationUnit au;
            au.first_iam_page = { PRIMARY_FILE_ID, pg };
            au.first_page = iam_start_page(page);

            // The IAM page header area contains the allocation unit ID
            // which links back to a specific hobt/partition
            if (pg + 96 + 8 < static_cast<int32_t>(PAGE_SIZE)) {
                std::memcpy(&au.allocation_unit_id, page + 96 + 8, 8);
            }

            alloc_units_.push_back(au);
            LOG_DEBUG("  IAM page %d: start=(%d:%d) auid=%lld",
                      pg, au.first_page.file_id, au.first_page.page_id,
                      (long long)au.allocation_unit_id);
        }
    }

    LOG_INFO("Found %zu allocation units via IAM scan", alloc_units_.size());
    return true;
}

bool CatalogReader::scan_rowset_allocunit_mapping() {
    LOG_DEBUG("Building object_id -> page header obj_id mapping...");

    // Step 1: Scan sysrowsets (page header obj_id=5) to build
    //         rowsetid (hobt_id) -> object_id mapping
    std::unordered_map<int64_t, int32_t> hobt_to_objid;

    uint8_t page[PAGE_SIZE];
    for (int32_t pg = 1; pg < 1000; ++pg) {
        if (!page_provider_(PRIMARY_FILE_ID, pg, page)) continue;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));
        if (hdr.type != static_cast<uint8_t>(PageType::Data)) continue;
        if (hdr.obj_id != OBJ_SYSROWSETS) continue;
        if (hdr.slot_count == 0) continue;

        for (int slot = 0; slot < hdr.slot_count; ++slot) {
            uint16_t offset = get_slot_offset(page, slot);
            if (offset < PAGE_HEADER_SIZE || offset >= PAGE_SIZE - 20) continue;

            const uint8_t* rec = page + offset;
            uint16_t fixed_end;
            std::memcpy(&fixed_end, rec + 2, 2);
            if (fixed_end < 21) continue;

            // sysrowsets layout:
            //   offset 4:  rowsetid (int64) = hobt_id
            //   offset 12: ownertype (tinyint)
            //   offset 13: idmajor (int32) = object_id
            //   offset 17: idminor (int32) = index_id
            int64_t rowsetid;
            std::memcpy(&rowsetid, rec + 4, 8);

            int32_t idmajor;
            std::memcpy(&idmajor, rec + 13, 4);

            int32_t idminor;
            std::memcpy(&idminor, rec + 17, 4);

            // Only map the heap/clustered index rowset (idminor = 0 or 1)
            if (idminor <= 1 && idmajor > 0) {
                hobt_to_objid[rowsetid] = idmajor;
            }
        }
    }

    LOG_DEBUG("Found %zu rowset->object mappings", hobt_to_objid.size());

    // Step 2: Scan sysallocunits (page header obj_id=7) to build
    //         the final object_id -> page header m_objId mapping
    for (int32_t pg = 1; pg < 1000; ++pg) {
        if (!page_provider_(PRIMARY_FILE_ID, pg, page)) continue;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));
        if (hdr.type != static_cast<uint8_t>(PageType::Data)) continue;
        if (hdr.obj_id != OBJ_SYSALLOCUNITS) continue;
        if (hdr.slot_count == 0) continue;

        for (int slot = 0; slot < hdr.slot_count; ++slot) {
            uint16_t offset = get_slot_offset(page, slot);
            if (offset < PAGE_HEADER_SIZE || offset >= PAGE_SIZE - 20) continue;

            const uint8_t* rec = page + offset;
            uint16_t fixed_end;
            std::memcpy(&fixed_end, rec + 2, 2);
            if (fixed_end < 21) continue;

            // sysallocunits layout:
            //   offset 4:  auid (int64) = allocation_unit_id
            //   offset 12: type (tinyint) - 1=IN_ROW_DATA
            //   offset 13: container_id (int64) = hobt_id
            int64_t auid;
            std::memcpy(&auid, rec + 4, 8);

            uint8_t au_type = rec[12];

            int64_t container_id;
            std::memcpy(&container_id, rec + 13, 8);

            // Only care about IN_ROW_DATA allocation units
            if (au_type != 1) continue;

            // The page header m_objId = (auid >> 16) & 0xFFFF
            uint32_t page_objid = static_cast<uint32_t>((auid >> 16) & 0xFFFF);

            auto it = hobt_to_objid.find(container_id);
            if (it != hobt_to_objid.end()) {
                int32_t object_id = it->second;
                obj_to_page_objid_[object_id] = page_objid;
            }
        }
    }

    LOG_INFO("Built %zu object -> page obj_id mappings", obj_to_page_objid_.size());
    for (auto& [oid, pobjid] : obj_to_page_objid_) {
        auto obj_it = objects_.find(oid);
        if (obj_it != objects_.end() && obj_it->second.type[0] == 'U') {
            LOG_DEBUG("  %s (object_id=%d) -> page_obj_id=%u",
                      obj_it->second.name.c_str(), oid, pobjid);
        }
    }

    return true;
}

uint32_t CatalogReader::get_page_obj_id(int32_t object_id) const {
    auto it = obj_to_page_objid_.find(object_id);
    return (it != obj_to_page_objid_.end()) ? it->second : 0;
}

bool CatalogReader::resolve_table(const std::string& schema_name,
                                   const std::string& table_name,
                                   TableSchema& out) const {
    // Find the object by name
    for (auto& [id, obj] : objects_) {
        // Check type is user table
        if (obj.type.size() < 1 || obj.type[0] != 'U') continue;

        // Check name match (case-insensitive)
        if (obj.name.size() != table_name.size()) continue;
        bool name_match = true;
        for (size_t i = 0; i < obj.name.size(); ++i) {
            if (tolower(obj.name[i]) != tolower(table_name[i])) {
                name_match = false;
                break;
            }
        }
        if (!name_match) continue;

        // Check schema match
        std::string obj_schema = schema_name_for_id(obj.schema_id);
        if (!obj_schema.empty() && !schema_name.empty()) {
            bool schema_match = true;
            if (obj_schema.size() != schema_name.size()) schema_match = false;
            else {
                for (size_t i = 0; i < obj_schema.size(); ++i) {
                    if (tolower(obj_schema[i]) != tolower(schema_name[i])) {
                        schema_match = false;
                        break;
                    }
                }
            }
            if (!schema_match) continue;
        }

        // Found the table
        out.object_id   = id;
        out.schema_name = obj_schema.empty() ? schema_name : obj_schema;
        out.table_name  = obj.name;

        // Populate columns
        auto col_it = columns_.find(id);
        if (col_it != columns_.end()) {
            for (auto& sc : col_it->second) {
                ColumnDef cd;
                cd.column_id   = sc.column_id;
                cd.name        = sc.name;
                cd.type        = static_cast<SqlType>(sc.system_type_id);
                cd.max_length  = sc.max_length;
                cd.precision   = sc.precision;
                cd.scale       = sc.scale;
                cd.is_nullable = sc.is_nullable;
                cd.is_identity = sc.is_identity;
                cd.leaf_offset = sc.leaf_offset;
                out.columns.push_back(cd);
            }
        }

        // Check for clustered index
        auto idx_it = indexes_.find(id);
        if (idx_it != indexes_.end()) {
            for (auto& idx : idx_it->second) {
                if (idx.type == 1) { out.is_heap = false; break; }
            }
        }

        LOG_INFO("Resolved table: %s (object_id=%d, %zu columns, %s)",
                 out.qualified_name().c_str(), out.object_id,
                 out.columns.size(), out.is_heap ? "heap" : "clustered");
        return true;
    }

    return false;
}

std::vector<SystemObject> CatalogReader::list_user_tables() const {
    std::vector<SystemObject> result;
    for (auto& [id, obj] : objects_) {
        if (obj.type.size() >= 1 && obj.type[0] == 'U') {
            result.push_back(obj);
        }
    }
    std::sort(result.begin(), result.end(),
              [](const SystemObject& a, const SystemObject& b) {
                  return a.name < b.name;
              });
    return result;
}

std::vector<SystemAllocationUnit>
CatalogReader::get_allocation_units(int32_t object_id) const {
    // Match allocation units by looking for container_id associations
    // In a full implementation, we'd join sysrowsets to map object_id
    // to hobt_id to allocation_unit. Here we return all AUs for now
    // and let the caller filter by page content.
    (void)object_id;
    return alloc_units_;
}

std::vector<PageId> CatalogReader::get_iam_chain(const PageId& first_iam) const {
    std::vector<PageId> chain;
    chain.push_back(first_iam);

    uint8_t page[PAGE_SIZE];
    PageId current = first_iam;
    int max_follow = 10000;

    while (--max_follow > 0) {
        if (!page_provider_(current.file_id, current.page_id, page)) break;

        PageHeader hdr;
        std::memcpy(&hdr, page, sizeof(hdr));

        PageId next = get_next_page(hdr);
        if (next.is_null()) break;

        chain.push_back(next);
        current = next;
    }

    return chain;
}

std::string CatalogReader::schema_name_for_id(int32_t schema_id) const {
    auto it = schema_names_.find(schema_id);
    if (it != schema_names_.end()) return it->second;
    return "dbo";
}

}  // namespace bakread
