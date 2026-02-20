#pragma once

#include "bakread/types.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>

namespace bakread {

// -------------------------------------------------------------------------
// RestoreAdapter -- Mode B: Restore .bak to SQL Server, extract via ODBC
//
// This is the reliable extraction path. It:
//   1. Connects to a SQL Server instance via ODBC
//   2. Determines backup contents (RESTORE HEADERONLY / FILELISTONLY)
//   3. Handles TDE certificate provisioning if needed
//   4. Restores the database to a temporary name
//   5. Queries the target table and streams rows
//   6. Drops the temporary database
//   7. Optionally cleans up certificates
// -------------------------------------------------------------------------

struct RestoreOptions {
    std::vector<std::string> bak_paths;  // One or more stripe files
    std::string target_server;         // SQL Server instance
    std::string target_database;       // Temp DB name (auto-generated if empty)
    std::string schema_name = "dbo";
    std::string table_name;
    std::vector<std::string> columns;  // Empty = all
    std::string where_clause;
    int64_t     max_rows = -1;
    int32_t     backupset = -1;

    // SQL Server Authentication (empty = Windows Auth)
    std::string sql_username;
    std::string sql_password;

    // TDE
    std::string tde_cert_pfx;       // PFX file OR .cer file
    std::string tde_cert_key;       // .pvk file (if using separate cert/key)
    std::string tde_cert_password;
    std::string master_key_password;
    bool        cleanup_keys = false;

    // Restore file relocation
    std::string data_file_path;   // If empty, uses SQL Server default
    std::string log_file_path;
};

using RowCallback = std::function<bool(const Row& row)>;

struct RestoreResult {
    bool        success     = false;
    uint64_t    rows_read   = 0;
    std::string error_message;
    TableSchema schema;
};

struct RestoreListTablesResult {
    bool success = false;
    std::vector<std::string> tables;  // schema.table format
    std::string error_message;
};

class OdbcConnection {
public:
    OdbcConnection();
    ~OdbcConnection();

    OdbcConnection(const OdbcConnection&) = delete;
    OdbcConnection& operator=(const OdbcConnection&) = delete;

    // Connect to SQL Server (empty username = Windows Auth)
    bool connect(const std::string& server, const std::string& database = "master",
                 const std::string& username = "", const std::string& password = "");

    // Execute a non-query SQL statement
    // If consume_results is true, consumes all result sets (needed for RESTORE)
    bool execute(const std::string& sql, bool consume_results = false);

    // Execute and fetch scalar string result
    bool query_scalar(const std::string& sql, std::string& result);

    // Execute and fetch scalar int result
    bool query_scalar_int(const std::string& sql, int64_t& result);

    // Execute a query and process rows via callback
    // The callback receives column values as strings
    bool query_rows(const std::string& sql,
                    const std::vector<ColumnDef>& schema,
                    RowCallback callback,
                    int64_t max_rows = -1);

    // Get last error message
    std::string last_error() const;

    // Check if connected
    bool is_connected() const { return connected_; }

    // Get the ODBC statement handle for advanced operations
    SQLHSTMT raw_stmt() const { return stmt_; }

private:
    bool alloc_handles();
    void free_handles();
    std::string get_diag(SQLSMALLINT handle_type, SQLHANDLE handle);

    // Convert ODBC SQL type to a RowValue
    RowValue fetch_column_value(SQLHSTMT stmt, int col_index,
                                const ColumnDef& col_def);

    SQLHENV  env_  = SQL_NULL_HENV;
    SQLHDBC  dbc_  = SQL_NULL_HDBC;
    SQLHSTMT stmt_ = SQL_NULL_HSTMT;
    bool     connected_ = false;
    std::string last_error_;
};

class RestoreAdapter {
public:
    explicit RestoreAdapter(const RestoreOptions& opts);
    ~RestoreAdapter();

    // Execute the full restore-extract-cleanup pipeline
    RestoreResult extract(RowCallback callback);

    // List all user tables in the backup (restores temporarily)
    RestoreListTablesResult list_tables();

    // Get the resolved schema (available after extract)
    const TableSchema& resolved_schema() const { return schema_; }

private:
    // Step 1: Connect and validate
    bool step_connect();

    // Step 2: Read backup metadata via RESTORE HEADERONLY / FILELISTONLY
    bool step_read_backup_info();

    // Step 3: Provision TDE certificates if needed
    bool step_provision_tde();

    // Step 4: Restore database
    bool step_restore_database();

    // Step 5: Read table schema from restored DB
    bool step_read_schema();

    // Step 6: Extract rows
    uint64_t step_extract_rows(RowCallback& callback);

    // Step 7: Cleanup (drop DB, remove certs)
    void step_cleanup();

    // Build the SELECT query
    std::string build_select_query() const;

    // Build the FROM DISK = N'...' clause for multi-file restores
    std::string build_from_disk_clause() const;

    // Generate a unique temp database name
    static std::string generate_temp_db_name();

    RestoreOptions opts_;
    TableSchema    schema_;
    BackupInfo     backup_info_;

    std::unique_ptr<OdbcConnection> conn_;
    std::string temp_db_name_;
    bool        db_restored_ = false;
    bool        cert_imported_ = false;
    bool        master_key_created_ = false;
};

}  // namespace bakread
