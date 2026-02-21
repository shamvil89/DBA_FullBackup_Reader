#include "bakread/bakread_api.h"
#include "bakread/direct_extractor.h"
#include "bakread/backup_header.h"
#include "bakread/backup_stream.h"
#include "bakread/types.h"

#include <cstring>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace {

struct ReaderState {
    std::vector<std::string> bak_paths;
    std::unique_ptr<bakread::DirectExtractor> extractor;
    bakread::DirectExtractorConfig config;
    
    std::string target_schema = "dbo";
    std::string target_table;
    std::vector<std::string> columns;
    int64_t max_rows = -1;
    
    std::string last_error;
    
    bakread::BackupInfo cached_info;
    bool info_loaded = false;
    bool info_parsed_directly = false;
    
    bakread::TableSchema cached_schema;
    bool schema_loaded = false;
    
    std::vector<BakTableInfoData> cached_tables;
    std::vector<std::string> cached_table_strings;
    
    std::vector<BakColumnInfo> cached_columns;
    std::vector<std::string> cached_column_names;
    
    BakProgressCallback progress_cb = nullptr;
    void* progress_user_data = nullptr;
    
    bool extracting = false;
    std::queue<bakread::Row> row_queue;
    std::vector<std::string> current_row_strings;
    std::vector<const char*> current_row_ptrs;
    std::mutex queue_mutex;
    bool extract_done = false;
    uint64_t rows_extracted = 0;
    
    BakBackupInfo api_info;
    std::string db_name_buf;
    std::string server_name_buf;
    std::string start_date_buf;
    std::string finish_date_buf;

    // Module cache
    std::vector<BakModuleInfo> cached_modules;
    std::vector<std::string> cached_module_strings;

    // Principal cache
    std::vector<BakPrincipalInfo> cached_principals;
    std::vector<std::string> cached_principal_strings;

    // Role member cache
    std::vector<BakRoleMemberInfo> cached_role_members;
    std::vector<std::string> cached_role_member_strings;

    // Permission cache
    std::vector<BakPermissionInfo> cached_permissions;
    std::vector<std::string> cached_permission_strings;
};

std::string row_value_to_string(const bakread::RowValue& val) {
    return std::visit([](auto&& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, bakread::NullValue>) {
            return "";
        } else if constexpr (std::is_same_v<T, bool>) {
            return v ? "1" : "0";
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                            std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
            return std::to_string(v);
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            return std::to_string(v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return v;
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            std::string hex = "0x";
            hex.reserve(2 + v.size() * 2);
            static const char hexchars[] = "0123456789ABCDEF";
            for (uint8_t b : v) {
                hex += hexchars[b >> 4];
                hex += hexchars[b & 0x0F];
            }
            return hex;
        } else if constexpr (std::is_same_v<T, bakread::SqlDecimal>) {
            return v.to_string();
        } else if constexpr (std::is_same_v<T, bakread::SqlGuid>) {
            return v.to_string();
        } else {
            return "";
        }
    }, val);
}

}  // namespace

extern "C" {

BAKREAD_API BakReadResult bakread_open(const char** bak_paths, int path_count, HBakReader* out_handle) {
    if (!bak_paths || path_count <= 0 || !out_handle) {
        return BAKREAD_ERROR_INVALID_HANDLE;
    }
    
    auto state = std::make_unique<ReaderState>();
    
    for (int i = 0; i < path_count; ++i) {
        if (bak_paths[i]) {
            state->bak_paths.emplace_back(bak_paths[i]);
        }
    }
    
    if (state->bak_paths.empty()) {
        return BAKREAD_ERROR_FILE_NOT_FOUND;
    }
    
    try {
        state->extractor = std::make_unique<bakread::DirectExtractor>(state->bak_paths, state->config);
    } catch (const std::exception& e) {
        state->last_error = e.what();
        *out_handle = nullptr;
        return BAKREAD_ERROR_INTERNAL;
    }
    
    *out_handle = reinterpret_cast<HBakReader>(state.release());
    return BAKREAD_OK;
}

BAKREAD_API void bakread_close(HBakReader handle) {
    if (handle) {
        delete reinterpret_cast<ReaderState*>(handle);
    }
}

BAKREAD_API const char* bakread_get_error(HBakReader handle) {
    if (!handle) return "Invalid handle";
    auto* state = reinterpret_cast<ReaderState*>(handle);
    return state->last_error.c_str();
}

BAKREAD_API BakReadResult bakread_get_info(HBakReader handle, BakBackupInfo* out_info) {
    if (!handle || !out_info) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    try {
        if (!state->info_loaded) {
            // Parse backup header directly to get info without running full extraction
            if (!state->info_parsed_directly) {
                bakread::BackupStream stream(state->bak_paths[0]);
                
                bakread::BackupHeaderParser parser(stream);
                if (!parser.parse()) {
                    state->last_error = "Failed to parse backup header";
                    return BAKREAD_ERROR_INVALID_FORMAT;
                }
                
                state->cached_info = parser.info();
                state->cached_info.file_path = state->bak_paths[0];
                state->info_parsed_directly = true;
            }
            state->info_loaded = true;
        }
        
        if (state->cached_info.backup_sets.empty()) {
            state->last_error = "No backup sets found";
            return BAKREAD_ERROR_INVALID_FORMAT;
        }
        
        const auto& bs = state->cached_info.backup_sets[0];
        
        state->db_name_buf = bs.database_name;
        state->server_name_buf = bs.server_name;
        state->start_date_buf = bs.backup_start_date;
        state->finish_date_buf = bs.backup_finish_date;
        
        state->api_info.database_name = state->db_name_buf.c_str();
        state->api_info.server_name = state->server_name_buf.c_str();
        state->api_info.backup_type = static_cast<BakBackupType>(bs.backup_type);
        state->api_info.compatibility_level = bs.compatibility_level;
        state->api_info.is_compressed = bs.is_compressed ? 1 : 0;
        state->api_info.is_encrypted = bs.is_encrypted ? 1 : 0;
        state->api_info.is_tde = bs.is_tde ? 1 : 0;
        state->api_info.backup_size = bs.backup_size;
        state->api_info.compressed_size = bs.compressed_size;
        state->api_info.backup_start_date = state->start_date_buf.c_str();
        state->api_info.backup_finish_date = state->finish_date_buf.c_str();
        
        *out_info = state->api_info;
        return BAKREAD_OK;
        
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API BakReadResult bakread_list_tables(HBakReader handle, BakTableInfoData** out_tables, int* out_count) {
    if (!handle || !out_tables || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    try {
        auto result = state->extractor->list_tables();
        if (!result.success) {
            state->last_error = result.error_message;
            return BAKREAD_ERROR_INTERNAL;
        }
        
        state->cached_tables.clear();
        state->cached_table_strings.clear();
        state->cached_table_strings.reserve(result.tables.size() * 3);
        state->cached_tables.reserve(result.tables.size());
        
        for (const auto& t : result.tables) {
            size_t base = state->cached_table_strings.size();
            state->cached_table_strings.push_back(t.schema_name);
            state->cached_table_strings.push_back(t.table_name);
            state->cached_table_strings.push_back(t.full_name);
            
            BakTableInfoData info;
            info.schema_name = state->cached_table_strings[base].c_str();
            info.table_name = state->cached_table_strings[base + 1].c_str();
            info.full_name = state->cached_table_strings[base + 2].c_str();
            info.object_id = t.object_id;
            info.row_count = t.row_count;
            info.page_count = t.page_count;
            state->cached_tables.push_back(info);
        }
        
        *out_tables = state->cached_tables.data();
        *out_count = static_cast<int>(state->cached_tables.size());
        return BAKREAD_OK;
        
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_table_list(BakTableInfoData* tables, int count) {
    (void)tables;
    (void)count;
}

BAKREAD_API BakReadResult bakread_set_table(HBakReader handle, const char* schema, const char* table) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->target_schema = schema ? schema : "dbo";
    state->target_table = table ? table : "";
    state->extractor->set_table(state->target_schema, state->target_table);
    state->schema_loaded = false;
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_set_columns(HBakReader handle, const char** columns, int column_count) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->columns.clear();
    for (int i = 0; i < column_count; ++i) {
        if (columns[i]) {
            state->columns.emplace_back(columns[i]);
        }
    }
    state->extractor->set_columns(state->columns);
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_set_max_rows(HBakReader handle, int64_t max_rows) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->max_rows = max_rows;
    state->extractor->set_max_rows(max_rows);
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_set_indexed_mode(HBakReader handle, int enabled, size_t cache_mb) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->config.use_indexed_mode = enabled != 0;
    state->config.cache_size_mb = cache_mb > 0 ? cache_mb : 256;
    
    try {
        state->extractor = std::make_unique<bakread::DirectExtractor>(state->bak_paths, state->config);
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_set_progress_callback(HBakReader handle, BakProgressCallback cb, void* user_data) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->progress_cb = cb;
    state->progress_user_data = user_data;
    
    if (cb) {
        state->extractor->set_progress_callback([state](const bakread::Progress& p) {
            if (state->progress_cb) {
                state->progress_cb(p.bytes_processed, p.bytes_total, p.rows_exported, p.pct, state->progress_user_data);
            }
        });
    } else {
        state->extractor->set_progress_callback(nullptr);
    }
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_get_schema(HBakReader handle, BakColumnInfo** out_columns, int* out_count) {
    if (!handle || !out_columns || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    try {
        const auto& schema = state->extractor->resolved_schema();
        
        state->cached_columns.clear();
        state->cached_column_names.clear();
        state->cached_column_names.reserve(schema.columns.size());
        state->cached_columns.reserve(schema.columns.size());
        
        for (const auto& col : schema.columns) {
            state->cached_column_names.push_back(col.name);
            
            BakColumnInfo info;
            info.name = state->cached_column_names.back().c_str();
            info.type = static_cast<BakSqlType>(col.type);
            info.max_length = col.max_length;
            info.precision = col.precision;
            info.scale = col.scale;
            info.is_nullable = col.is_nullable ? 1 : 0;
            info.is_identity = col.is_identity ? 1 : 0;
            info.is_computed = col.is_computed ? 1 : 0;
            state->cached_columns.push_back(info);
        }
        
        *out_columns = state->cached_columns.data();
        *out_count = static_cast<int>(state->cached_columns.size());
        return BAKREAD_OK;
        
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_schema(BakColumnInfo* columns, int count) {
    (void)columns;
    (void)count;
}

BAKREAD_API BakReadResult bakread_extract(HBakReader handle, BakRowCallback callback, void* user_data, uint64_t* out_row_count) {
    if (!handle || !callback) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    // Store the callback and user_data so we can also update schema info during extraction
    bool first_row = true;
    
    try {
        std::vector<std::string> row_strings;
        std::vector<const char*> row_ptrs;
        
        auto result = state->extractor->extract([&](const bakread::Row& row) -> bool {
            row_strings.clear();
            row_ptrs.clear();
            row_strings.reserve(row.size());
            row_ptrs.reserve(row.size());
            
            for (const auto& val : row) {
                row_strings.push_back(row_value_to_string(val));
            }
            for (const auto& s : row_strings) {
                row_ptrs.push_back(s.c_str());
            }
            
            return callback(row_ptrs.data(), static_cast<int>(row_ptrs.size()), user_data) == 0;
        });
        
        if (out_row_count) {
            *out_row_count = result.rows_read;
        }
        
        if (!result.success) {
            state->last_error = result.error_message;
            if (result.tde_detected) return BAKREAD_ERROR_TDE_DETECTED;
            if (result.encryption_detected) return BAKREAD_ERROR_ENCRYPTION_DETECTED;
            return BAKREAD_ERROR_INTERNAL;
        }
        
        return BAKREAD_OK;
        
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API BakReadResult bakread_begin_extract(HBakReader handle) {
    if (!handle) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->extracting = true;
    state->extract_done = false;
    state->rows_extracted = 0;
    
    while (!state->row_queue.empty()) {
        state->row_queue.pop();
    }
    
    return BAKREAD_OK;
}

BAKREAD_API BakReadResult bakread_next_row(HBakReader handle, const char*** out_values, int* out_column_count) {
    if (!handle || !out_values || !out_column_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    if (!state->extracting) {
        state->last_error = "Extraction not started. Call bakread_begin_extract first.";
        return BAKREAD_ERROR_INTERNAL;
    }
    
    if (state->extract_done) {
        return BAKREAD_ERROR_NO_MORE_ROWS;
    }
    
    try {
        bool got_row = false;
        
        auto result = state->extractor->extract([&](const bakread::Row& row) -> bool {
            state->current_row_strings.clear();
            state->current_row_ptrs.clear();
            state->current_row_strings.reserve(row.size());
            state->current_row_ptrs.reserve(row.size());
            
            for (const auto& val : row) {
                state->current_row_strings.push_back(row_value_to_string(val));
            }
            for (const auto& s : state->current_row_strings) {
                state->current_row_ptrs.push_back(s.c_str());
            }
            
            state->rows_extracted++;
            got_row = true;
            return false;  // Stop after first row for streaming mode
        });
        
        if (got_row) {
            *out_values = state->current_row_ptrs.data();
            *out_column_count = static_cast<int>(state->current_row_ptrs.size());
            return BAKREAD_OK;
        } else {
            state->extract_done = true;
            return BAKREAD_ERROR_NO_MORE_ROWS;
        }
        
    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_end_extract(HBakReader handle) {
    if (!handle) return;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    state->extracting = false;
    state->extract_done = true;
}

BAKREAD_API BakReadResult bakread_export_csv(HBakReader handle, const char* output_path, const char* delimiter) {
    if (!handle || !output_path) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    (void)delimiter;
    state->last_error = "Direct CSV export not yet implemented. Use bakread_extract with callback.";
    return BAKREAD_ERROR_INTERNAL;
}

BAKREAD_API BakReadResult bakread_export_json(HBakReader handle, const char* output_path) {
    if (!handle || !output_path) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);
    
    state->last_error = "Direct JSON export not yet implemented. Use bakread_extract with callback.";
    return BAKREAD_ERROR_INTERNAL;
}

// -------------------------------------------------------------------------
// Module (Stored Procedures, Functions, Views) API
// -------------------------------------------------------------------------

static const char* get_module_type_desc(const std::string& type) {
    if (type.size() < 2) return "UNKNOWN";
    if (type[0] == 'P' && type[1] == ' ') return "SQL_STORED_PROCEDURE";
    if (type[0] == 'F' && type[1] == 'N') return "SQL_SCALAR_FUNCTION";
    if (type[0] == 'I' && type[1] == 'F') return "SQL_INLINE_TABLE_VALUED_FUNCTION";
    if (type[0] == 'T' && type[1] == 'F') return "SQL_TABLE_VALUED_FUNCTION";
    if (type[0] == 'V' && type[1] == ' ') return "VIEW";
    return "UNKNOWN";
}

BAKREAD_API BakReadResult bakread_list_modules(HBakReader handle, BakModuleInfo** out_modules, int* out_count) {
    if (!handle || !out_modules || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);

    try {
        auto modules = state->extractor->list_modules();

        state->cached_modules.clear();
        state->cached_module_strings.clear();
        state->cached_module_strings.reserve(modules.size() * 5);
        state->cached_modules.reserve(modules.size());

        for (const auto& m : modules) {
            size_t base = state->cached_module_strings.size();
            state->cached_module_strings.push_back(m.schema_name);
            state->cached_module_strings.push_back(m.name);
            state->cached_module_strings.push_back(m.type);
            state->cached_module_strings.push_back(m.definition);

            BakModuleInfo info;
            info.object_id = m.object_id;
            info.schema_name = state->cached_module_strings[base].c_str();
            info.name = state->cached_module_strings[base + 1].c_str();
            info.type = state->cached_module_strings[base + 2].c_str();
            info.type_desc = get_module_type_desc(m.type);
            info.definition = state->cached_module_strings[base + 3].c_str();
            state->cached_modules.push_back(info);
        }

        *out_modules = state->cached_modules.data();
        *out_count = static_cast<int>(state->cached_modules.size());
        return BAKREAD_OK;

    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_module_list(BakModuleInfo* modules, int count) {
    (void)modules;
    (void)count;
}

// -------------------------------------------------------------------------
// Security/Permissions API
// -------------------------------------------------------------------------

static const char* get_principal_type_desc(const std::string& type) {
    if (type.empty()) return "UNKNOWN";
    switch (type[0]) {
        case 'S': return "SQL_USER";
        case 'U': return "WINDOWS_USER";
        case 'G': return "WINDOWS_GROUP";
        case 'R': return "DATABASE_ROLE";
        case 'A': return "APPLICATION_ROLE";
        case 'C': return "CERTIFICATE_MAPPED_USER";
        case 'K': return "ASYMMETRIC_KEY_MAPPED_USER";
        case 'X': return "EXTERNAL_GROUP";
        case 'E': return "EXTERNAL_USER";
        default: return "UNKNOWN";
    }
}

BAKREAD_API BakReadResult bakread_list_principals(HBakReader handle, BakPrincipalInfo** out_principals, int* out_count) {
    if (!handle || !out_principals || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);

    try {
        auto principals = state->extractor->list_principals();

        state->cached_principals.clear();
        state->cached_principal_strings.clear();
        state->cached_principal_strings.reserve(principals.size() * 3);
        state->cached_principals.reserve(principals.size());

        for (const auto& p : principals) {
            size_t base = state->cached_principal_strings.size();
            state->cached_principal_strings.push_back(p.name);
            state->cached_principal_strings.push_back(p.type);
            state->cached_principal_strings.push_back(p.default_schema);

            BakPrincipalInfo info;
            info.principal_id = p.principal_id;
            info.name = state->cached_principal_strings[base].c_str();
            info.type = state->cached_principal_strings[base + 1].c_str();
            info.type_desc = get_principal_type_desc(p.type);
            info.owning_principal_id = p.owning_principal_id;
            info.default_schema = state->cached_principal_strings[base + 2].c_str();
            info.is_fixed_role = p.is_fixed_role ? 1 : 0;
            state->cached_principals.push_back(info);
        }

        *out_principals = state->cached_principals.data();
        *out_count = static_cast<int>(state->cached_principals.size());
        return BAKREAD_OK;

    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_principal_list(BakPrincipalInfo* principals, int count) {
    (void)principals;
    (void)count;
}

BAKREAD_API BakReadResult bakread_list_role_members(HBakReader handle, BakRoleMemberInfo** out_members, int* out_count) {
    if (!handle || !out_members || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);

    try {
        auto members = state->extractor->list_role_members();

        state->cached_role_members.clear();
        state->cached_role_member_strings.clear();
        state->cached_role_member_strings.reserve(members.size() * 2);
        state->cached_role_members.reserve(members.size());

        for (const auto& m : members) {
            size_t base = state->cached_role_member_strings.size();
            state->cached_role_member_strings.push_back(m.role_name);
            state->cached_role_member_strings.push_back(m.member_name);

            BakRoleMemberInfo info;
            info.role_principal_id = m.role_principal_id;
            info.member_principal_id = m.member_principal_id;
            info.role_name = state->cached_role_member_strings[base].c_str();
            info.member_name = state->cached_role_member_strings[base + 1].c_str();
            state->cached_role_members.push_back(info);
        }

        *out_members = state->cached_role_members.data();
        *out_count = static_cast<int>(state->cached_role_members.size());
        return BAKREAD_OK;

    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_role_member_list(BakRoleMemberInfo* members, int count) {
    (void)members;
    (void)count;
}

static const char* get_permission_class_desc(int32_t class_type) {
    switch (class_type) {
        case 0: return "DATABASE";
        case 1: return "OBJECT_OR_COLUMN";
        case 3: return "SCHEMA";
        case 4: return "DATABASE_PRINCIPAL";
        case 5: return "ASSEMBLY";
        case 6: return "TYPE";
        case 10: return "XML_SCHEMA_COLLECTION";
        case 15: return "MESSAGE_TYPE";
        case 16: return "SERVICE_CONTRACT";
        case 17: return "SERVICE";
        case 18: return "REMOTE_SERVICE_BINDING";
        case 19: return "ROUTE";
        case 23: return "FULLTEXT_CATALOG";
        case 24: return "SYMMETRIC_KEY";
        case 25: return "CERTIFICATE";
        case 26: return "ASYMMETRIC_KEY";
        default: return "UNKNOWN";
    }
}

static const char* get_permission_state_desc(const std::string& state) {
    if (state.empty()) return "GRANT";
    switch (state[0]) {
        case 'G': return "GRANT";
        case 'D': return "DENY";
        case 'R': return "REVOKE";
        case 'W': return "GRANT_WITH_GRANT_OPTION";
        default: return "GRANT";
    }
}

BAKREAD_API BakReadResult bakread_list_permissions(HBakReader handle, BakPermissionInfo** out_permissions, int* out_count) {
    if (!handle || !out_permissions || !out_count) return BAKREAD_ERROR_INVALID_HANDLE;
    auto* state = reinterpret_cast<ReaderState*>(handle);

    try {
        auto permissions = state->extractor->list_permissions();

        state->cached_permissions.clear();
        state->cached_permission_strings.clear();
        state->cached_permission_strings.reserve(permissions.size() * 5);
        state->cached_permissions.reserve(permissions.size());

        for (const auto& p : permissions) {
            size_t base = state->cached_permission_strings.size();
            state->cached_permission_strings.push_back(p.permission_name);
            state->cached_permission_strings.push_back(p.grantee_name);
            state->cached_permission_strings.push_back(p.grantor_name);
            state->cached_permission_strings.push_back(p.object_name);
            state->cached_permission_strings.push_back(p.schema_name);

            BakPermissionInfo info;
            info.class_type = p.class_type;
            info.class_desc = get_permission_class_desc(p.class_type);
            info.major_id = p.major_id;
            info.minor_id = p.minor_id;
            info.permission_name = state->cached_permission_strings[base].c_str();
            info.state = get_permission_state_desc(p.state);
            info.grantee_name = state->cached_permission_strings[base + 1].c_str();
            info.grantor_name = state->cached_permission_strings[base + 2].c_str();
            info.object_name = state->cached_permission_strings[base + 3].c_str();
            info.schema_name = state->cached_permission_strings[base + 4].c_str();
            state->cached_permissions.push_back(info);
        }

        *out_permissions = state->cached_permissions.data();
        *out_count = static_cast<int>(state->cached_permissions.size());
        return BAKREAD_OK;

    } catch (const std::exception& e) {
        state->last_error = e.what();
        return BAKREAD_ERROR_INTERNAL;
    }
}

BAKREAD_API void bakread_free_permission_list(BakPermissionInfo* permissions, int count) {
    (void)permissions;
    (void)count;
}

BAKREAD_API const char* bakread_version(void) {
    return "1.0.0";
}

}  // extern "C"
