#pragma once

#ifdef _WIN32
#   ifdef BAKREAD_API_EXPORTS
#       define BAKREAD_API __declspec(dllexport)
#   else
#       define BAKREAD_API __declspec(dllimport)
#   endif
#else
#   define BAKREAD_API __attribute__((visibility("default")))
#endif

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Handle types (opaque pointers)
typedef struct BakReadHandle* HBakReader;
typedef struct BakTableInfo* HTableInfo;
typedef struct BakRowHandle* HRow;

// Result codes
typedef enum BakReadResult {
    BAKREAD_OK = 0,
    BAKREAD_ERROR_FILE_NOT_FOUND = 1,
    BAKREAD_ERROR_INVALID_FORMAT = 2,
    BAKREAD_ERROR_TDE_DETECTED = 3,
    BAKREAD_ERROR_ENCRYPTION_DETECTED = 4,
    BAKREAD_ERROR_TABLE_NOT_FOUND = 5,
    BAKREAD_ERROR_INTERNAL = 6,
    BAKREAD_ERROR_INVALID_HANDLE = 7,
    BAKREAD_ERROR_NO_MORE_ROWS = 8,
} BakReadResult;

// SQL type identifiers (matches SqlType enum)
typedef enum BakSqlType {
    BAKSQL_UNKNOWN = 0,
    BAKSQL_TINYINT = 48,
    BAKSQL_SMALLINT = 52,
    BAKSQL_INT = 56,
    BAKSQL_BIGINT = 127,
    BAKSQL_BIT = 104,
    BAKSQL_FLOAT = 62,
    BAKSQL_REAL = 59,
    BAKSQL_DECIMAL = 106,
    BAKSQL_NUMERIC = 108,
    BAKSQL_MONEY = 60,
    BAKSQL_SMALLMONEY = 122,
    BAKSQL_DATE = 40,
    BAKSQL_TIME = 41,
    BAKSQL_DATETIME = 61,
    BAKSQL_DATETIME2 = 42,
    BAKSQL_SMALLDATETIME = 58,
    BAKSQL_DATETIMEOFFSET = 43,
    BAKSQL_CHAR = 175,
    BAKSQL_VARCHAR = 167,
    BAKSQL_NCHAR = 239,
    BAKSQL_NVARCHAR = 231,
    BAKSQL_TEXT = 35,
    BAKSQL_NTEXT = 99,
    BAKSQL_BINARY = 173,
    BAKSQL_VARBINARY = 165,
    BAKSQL_IMAGE = 34,
    BAKSQL_UNIQUEID = 36,
    BAKSQL_XML = 241,
    BAKSQL_TIMESTAMP = 189,
    BAKSQL_SQL_VARIANT = 98,
} BakSqlType;

// Backup type
typedef enum BakBackupType {
    BAKTYPE_UNKNOWN = 0,
    BAKTYPE_FULL = 1,
    BAKTYPE_DIFFERENTIAL = 2,
    BAKTYPE_LOG = 3,
} BakBackupType;

// Backup info structure
typedef struct BakBackupInfo {
    const char* database_name;
    const char* server_name;
    BakBackupType backup_type;
    int32_t compatibility_level;
    int is_compressed;
    int is_encrypted;
    int is_tde;
    uint64_t backup_size;
    uint64_t compressed_size;
    const char* backup_start_date;
    const char* backup_finish_date;
} BakBackupInfo;

// Table info structure
typedef struct BakTableInfoData {
    const char* schema_name;
    const char* table_name;
    const char* full_name;
    int32_t object_id;
    int64_t row_count;
    int64_t page_count;
} BakTableInfoData;

// Column info structure
typedef struct BakColumnInfo {
    const char* name;
    BakSqlType type;
    int16_t max_length;
    uint8_t precision;
    uint8_t scale;
    int is_nullable;
    int is_identity;
    int is_computed;
} BakColumnInfo;

// Module info structure (stored procedures, functions, views)
typedef struct BakModuleInfo {
    int32_t object_id;
    const char* schema_name;
    const char* name;
    const char* type;           // "P " = procedure, "FN" = scalar function, "IF" = inline TVF, "TF" = TVF, "V " = view
    const char* type_desc;      // Human-readable: "PROCEDURE", "SCALAR_FUNCTION", etc.
    const char* definition;     // T-SQL definition text
} BakModuleInfo;

// Database principal info structure
typedef struct BakPrincipalInfo {
    int32_t principal_id;
    const char* name;
    const char* type;           // "S" = SQL user, "U" = Windows user, "R" = role, "A" = application role
    const char* type_desc;      // Human-readable
    int32_t owning_principal_id;
    const char* default_schema;
    int is_fixed_role;
} BakPrincipalInfo;

// Role membership structure
typedef struct BakRoleMemberInfo {
    int32_t role_principal_id;
    int32_t member_principal_id;
    const char* role_name;
    const char* member_name;
} BakRoleMemberInfo;

// Database permission structure
typedef struct BakPermissionInfo {
    int32_t class_type;         // 0=database, 1=object, 3=schema
    const char* class_desc;     // "DATABASE", "OBJECT_OR_COLUMN", "SCHEMA"
    int32_t major_id;           // object_id or schema_id
    int32_t minor_id;           // column_id (0 if N/A)
    const char* permission_name;// "SELECT", "EXECUTE", etc.
    const char* state;          // "GRANT", "DENY", "REVOKE", "GRANT_WITH_GRANT_OPTION"
    const char* grantee_name;
    const char* grantor_name;
    const char* object_name;    // Name of object (if applicable)
    const char* schema_name;    // Schema of object (if applicable)
} BakPermissionInfo;

// Progress callback
typedef void (*BakProgressCallback)(uint64_t bytes_processed, uint64_t bytes_total,
                                     uint64_t rows_exported, double pct, void* user_data);

// Row callback - return 0 to continue, non-zero to stop
typedef int (*BakRowCallback)(const char** values, int column_count, void* user_data);

// -------------------------------------------------------------------------
// API Functions
// -------------------------------------------------------------------------

// Create a reader handle for one or more .bak files
BAKREAD_API BakReadResult bakread_open(const char** bak_paths, int path_count, HBakReader* out_handle);

// Close and free the reader handle
BAKREAD_API void bakread_close(HBakReader handle);

// Get the last error message
BAKREAD_API const char* bakread_get_error(HBakReader handle);

// Get backup info
BAKREAD_API BakReadResult bakread_get_info(HBakReader handle, BakBackupInfo* out_info);

// List all user tables
BAKREAD_API BakReadResult bakread_list_tables(HBakReader handle, BakTableInfoData** out_tables, int* out_count);

// Free table list
BAKREAD_API void bakread_free_table_list(BakTableInfoData* tables, int count);

// Set extraction options
BAKREAD_API BakReadResult bakread_set_table(HBakReader handle, const char* schema, const char* table);
BAKREAD_API BakReadResult bakread_set_columns(HBakReader handle, const char** columns, int column_count);
BAKREAD_API BakReadResult bakread_set_max_rows(HBakReader handle, int64_t max_rows);
BAKREAD_API BakReadResult bakread_set_indexed_mode(HBakReader handle, int enabled, size_t cache_mb);
BAKREAD_API BakReadResult bakread_set_progress_callback(HBakReader handle, BakProgressCallback cb, void* user_data);

// Get table schema (after setting table)
BAKREAD_API BakReadResult bakread_get_schema(HBakReader handle, BakColumnInfo** out_columns, int* out_count);
BAKREAD_API void bakread_free_schema(BakColumnInfo* columns, int count);

// Extract rows using callback
BAKREAD_API BakReadResult bakread_extract(HBakReader handle, BakRowCallback callback, void* user_data, uint64_t* out_row_count);

// Streaming row extraction - initialize extraction
BAKREAD_API BakReadResult bakread_begin_extract(HBakReader handle);

// Streaming row extraction - get next row (returns values as strings)
// Returns BAKREAD_OK if row available, BAKREAD_ERROR_NO_MORE_ROWS when done
BAKREAD_API BakReadResult bakread_next_row(HBakReader handle, const char*** out_values, int* out_column_count);

// End extraction and cleanup
BAKREAD_API void bakread_end_extract(HBakReader handle);

// Export directly to file
BAKREAD_API BakReadResult bakread_export_csv(HBakReader handle, const char* output_path, const char* delimiter);
BAKREAD_API BakReadResult bakread_export_json(HBakReader handle, const char* output_path);

// -------------------------------------------------------------------------
// Module (Stored Procedures, Functions, Views) API
// -------------------------------------------------------------------------

// List all modules (stored procedures, functions, views)
BAKREAD_API BakReadResult bakread_list_modules(HBakReader handle, BakModuleInfo** out_modules, int* out_count);

// Free module list
BAKREAD_API void bakread_free_module_list(BakModuleInfo* modules, int count);

// -------------------------------------------------------------------------
// Security/Permissions API
// -------------------------------------------------------------------------

// List all database principals (users, roles)
BAKREAD_API BakReadResult bakread_list_principals(HBakReader handle, BakPrincipalInfo** out_principals, int* out_count);

// Free principal list
BAKREAD_API void bakread_free_principal_list(BakPrincipalInfo* principals, int count);

// List role memberships
BAKREAD_API BakReadResult bakread_list_role_members(HBakReader handle, BakRoleMemberInfo** out_members, int* out_count);

// Free role member list
BAKREAD_API void bakread_free_role_member_list(BakRoleMemberInfo* members, int count);

// List database permissions
BAKREAD_API BakReadResult bakread_list_permissions(HBakReader handle, BakPermissionInfo** out_permissions, int* out_count);

// Free permission list
BAKREAD_API void bakread_free_permission_list(BakPermissionInfo* permissions, int count);

// Version info
BAKREAD_API const char* bakread_version(void);

#ifdef __cplusplus
}
#endif
