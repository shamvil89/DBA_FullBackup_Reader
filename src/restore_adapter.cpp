#include "bakread/restore_adapter.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <random>
#include <sstream>
#include <thread>

namespace bakread {

// =========================================================================
// OdbcConnection
// =========================================================================

OdbcConnection::OdbcConnection() {
    alloc_handles();
}

OdbcConnection::~OdbcConnection() {
    free_handles();
}

bool OdbcConnection::alloc_handles() {
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
    if (!SQL_SUCCEEDED(ret)) return false;

    ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION,
                        reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3_80), 0);
    if (!SQL_SUCCEEDED(ret)) {
        SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION,
                      reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
    if (!SQL_SUCCEEDED(ret)) {
        last_error_ = "Failed to allocate ODBC connection handle";
        return false;
    }

    // Set connection timeout
    SQLSetConnectAttr(dbc_, SQL_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(30), 0);

    return true;
}

void OdbcConnection::free_handles() {
    if (stmt_ != SQL_NULL_HSTMT) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_);
        stmt_ = SQL_NULL_HSTMT;
    }
    if (dbc_ != SQL_NULL_HDBC) {
        if (connected_) {
            SQLDisconnect(dbc_);
            connected_ = false;
        }
        SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
        dbc_ = SQL_NULL_HDBC;
    }
    if (env_ != SQL_NULL_HENV) {
        SQLFreeHandle(SQL_HANDLE_ENV, env_);
        env_ = SQL_NULL_HENV;
    }
}

bool OdbcConnection::connect(const std::string& server, const std::string& database,
                             const std::string& username, const std::string& password) {
    // Build connection string using ODBC Driver 18 (or 17 as fallback)
    // Use SQL auth if username provided, otherwise Windows Auth
    bool use_sql_auth = !username.empty();
    
    std::string conn_str =
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=" + server + ";"
        "DATABASE=" + database + ";";
    
    if (use_sql_auth) {
        conn_str += "UID=" + username + ";"
                    "PWD=" + password + ";";
        LOG_DEBUG("Using SQL Server Authentication for user: %s", username.c_str());
    } else {
        conn_str += "Trusted_Connection=yes;";
        LOG_DEBUG("Using Windows Authentication");
    }
    
    conn_str += "TrustServerCertificate=yes;"
                "Connection Timeout=30;";

    LOG_DEBUG("ODBC connection string: %s", 
              use_sql_auth ? "(credentials hidden)" : conn_str.c_str());

    SQLCHAR out_conn[1024];
    SQLSMALLINT out_len;

    SQLRETURN ret = SQLDriverConnectA(
        dbc_, nullptr,
        reinterpret_cast<SQLCHAR*>(const_cast<char*>(conn_str.c_str())),
        static_cast<SQLSMALLINT>(conn_str.size()),
        out_conn, sizeof(out_conn), &out_len,
        SQL_DRIVER_NOPROMPT);

    if (!SQL_SUCCEEDED(ret)) {
        last_error_ = get_diag(SQL_HANDLE_DBC, dbc_);

        // Retry with Driver 17
        conn_str =
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=" + server + ";"
            "DATABASE=" + database + ";";
            
        if (use_sql_auth) {
            conn_str += "UID=" + username + ";"
                        "PWD=" + password + ";";
        } else {
            conn_str += "Trusted_Connection=yes;";
        }
        conn_str += "TrustServerCertificate=yes;";

        ret = SQLDriverConnectA(
            dbc_, nullptr,
            reinterpret_cast<SQLCHAR*>(const_cast<char*>(conn_str.c_str())),
            static_cast<SQLSMALLINT>(conn_str.size()),
            out_conn, sizeof(out_conn), &out_len,
            SQL_DRIVER_NOPROMPT);

        if (!SQL_SUCCEEDED(ret)) {
            last_error_ = "Cannot connect to SQL Server '" + server +
                          "': " + get_diag(SQL_HANDLE_DBC, dbc_);
            return false;
        }
    }

    connected_ = true;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc_, &stmt_);
    if (!SQL_SUCCEEDED(ret)) {
        last_error_ = "Failed to allocate statement handle";
        return false;
    }

    LOG_INFO("Connected to SQL Server: %s", server.c_str());
    return true;
}

bool OdbcConnection::execute(const std::string& sql, bool consume_results) {
    if (!connected_) return false;

    LOG_DEBUG("Execute: %s", sql.c_str());

    // Free any previous results
    SQLFreeStmt(stmt_, SQL_CLOSE);

    SQLRETURN ret = SQLExecDirectA(
        stmt_,
        reinterpret_cast<SQLCHAR*>(const_cast<char*>(sql.c_str())),
        static_cast<SQLINTEGER>(sql.size()));

    if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
        last_error_ = get_diag(SQL_HANDLE_STMT, stmt_);
        LOG_ERROR("SQL execution failed: %s", last_error_.c_str());
        return false;
    }

    // For commands like RESTORE DATABASE that return multiple result sets,
    // we must consume all results before the connection can be used again.
    if (consume_results) {
        int result_count = 0;
        int row_count = 0;
        
        // Consume all rows in current result set
        SQLRETURN fr;
        while ((fr = SQLFetch(stmt_)) == SQL_SUCCESS || fr == SQL_SUCCESS_WITH_INFO) { 
            ++row_count; 
        }
        ++result_count;
        
        // Consume any additional result sets
        SQLRETURN mr;
        while ((mr = SQLMoreResults(stmt_)) == SQL_SUCCESS || mr == SQL_SUCCESS_WITH_INFO) {
            ++result_count;
            while ((fr = SQLFetch(stmt_)) == SQL_SUCCESS || fr == SQL_SUCCESS_WITH_INFO) { 
                ++row_count; 
            }
        }
        
        LOG_DEBUG("Consumed %d result sets, %d total rows (last more_results=%d)", 
                  result_count, row_count, (int)mr);
        SQLFreeStmt(stmt_, SQL_CLOSE);
    }

    return true;
}

bool OdbcConnection::query_scalar(const std::string& sql, std::string& result) {
    if (!execute(sql)) return false;

    SQLRETURN ret = SQLFetch(stmt_);
    if (!SQL_SUCCEEDED(ret)) {
        SQLFreeStmt(stmt_, SQL_CLOSE);
        return false;
    }

    char buf[4096];
    SQLLEN indicator;
    ret = SQLGetData(stmt_, 1, SQL_C_CHAR, buf, sizeof(buf), &indicator);

    SQLFreeStmt(stmt_, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret) || indicator == SQL_NULL_DATA) {
        result.clear();
        return true;
    }

    result = buf;
    return true;
}

bool OdbcConnection::query_scalar_int(const std::string& sql, int64_t& result) {
    if (!execute(sql)) return false;

    SQLRETURN ret = SQLFetch(stmt_);
    if (!SQL_SUCCEEDED(ret)) {
        SQLFreeStmt(stmt_, SQL_CLOSE);
        return false;
    }

    SQLLEN indicator;
    ret = SQLGetData(stmt_, 1, SQL_C_SBIGINT, &result, sizeof(result), &indicator);

    SQLFreeStmt(stmt_, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret) || indicator == SQL_NULL_DATA) {
        result = 0;
        return true;
    }

    return true;
}

bool OdbcConnection::query_rows(const std::string& sql,
                                 const std::vector<ColumnDef>& schema,
                                 RowCallback callback,
                                 int64_t max_rows) {
    if (!execute(sql)) return false;

    int64_t row_count = 0;
    SQLRETURN ret;

    while ((ret = SQLFetch(stmt_)) == SQL_SUCCESS ||
           ret == SQL_SUCCESS_WITH_INFO) {

        Row row(schema.size());
        for (size_t i = 0; i < schema.size(); ++i) {
            row[i] = fetch_column_value(stmt_, static_cast<int>(i + 1), schema[i]);
        }

        if (!callback(row)) break;

        ++row_count;
        if (max_rows > 0 && row_count >= max_rows) break;
    }

    SQLFreeStmt(stmt_, SQL_CLOSE);
    return true;
}

RowValue OdbcConnection::fetch_column_value(SQLHSTMT stmt, int col_index,
                                             const ColumnDef& col_def) {
    SQLLEN indicator;

    switch (col_def.type) {
    case SqlType::TinyInt: {
        int8_t val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_STINYINT, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::SmallInt: {
        int16_t val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_SSHORT, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::Int: {
        int32_t val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_SLONG, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::BigInt: {
        int64_t val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_SBIGINT, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::Bit: {
        uint8_t val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_BIT, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return static_cast<bool>(val);
    }
    case SqlType::Float: {
        double val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_DOUBLE, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::Real: {
        float val;
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_FLOAT, &val, sizeof(val), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        return val;
    }
    case SqlType::Binary:
    case SqlType::VarBinary:
    case SqlType::Image:
    case SqlType::Timestamp: {
        // Fetch binary data in chunks
        std::vector<uint8_t> buf(65536);
        SQLRETURN ret = SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                                   SQL_C_BINARY, buf.data(),
                                   static_cast<SQLLEN>(buf.size()), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        if (!SQL_SUCCEEDED(ret)) return NullValue{};
        if (indicator > 0)
            buf.resize(std::min<size_t>(static_cast<size_t>(indicator), buf.size()));
        return buf;
    }
    case SqlType::UniqueId: {
        char buf[64];
        SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                   SQL_C_CHAR, buf, sizeof(buf), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        // Parse GUID string into SqlGuid
        return std::string(buf);
    }
    default: {
        // Fetch everything else as string (handles varchar, nvarchar,
        // datetime, decimal, etc.)
        char buf[65536];
        SQLRETURN ret = SQLGetData(stmt, static_cast<SQLUSMALLINT>(col_index),
                                   SQL_C_CHAR, buf, sizeof(buf), &indicator);
        if (indicator == SQL_NULL_DATA) return NullValue{};
        if (!SQL_SUCCEEDED(ret)) return NullValue{};
        return std::string(buf);
    }
    }
}

std::string OdbcConnection::last_error() const {
    return last_error_;
}

std::string OdbcConnection::get_diag(SQLSMALLINT handle_type, SQLHANDLE handle) {
    SQLCHAR state[8], msg[1024];
    SQLINTEGER native;
    SQLSMALLINT len;
    std::string result;

    for (SQLSMALLINT i = 1; ; ++i) {
        SQLRETURN ret = SQLGetDiagRecA(handle_type, handle, i,
                                        state, &native, msg, sizeof(msg), &len);
        if (!SQL_SUCCEEDED(ret)) break;
        if (!result.empty()) result += " | ";
        result += "[" + std::string(reinterpret_cast<char*>(state)) + "] " +
                  std::string(reinterpret_cast<char*>(msg));
    }
    return result;
}

// =========================================================================
// RestoreAdapter
// =========================================================================

RestoreAdapter::RestoreAdapter(const RestoreOptions& opts)
    : opts_(opts)
{
    if (opts_.target_database.empty())
        temp_db_name_ = generate_temp_db_name();
    else
        temp_db_name_ = opts_.target_database;
}

RestoreAdapter::~RestoreAdapter() {
    step_cleanup();
}

RestoreResult RestoreAdapter::extract(RowCallback callback) {
    RestoreResult result;

    try {
        LOG_INFO("=== Restore & Extract Mode (Mode B) ===");

        if (!step_connect()) {
            result.error_message = "Failed to connect to SQL Server: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_read_backup_info()) {
            result.error_message = "Failed to read backup metadata: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_provision_tde()) {
            result.error_message = "TDE certificate provisioning failed: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_restore_database()) {
            result.error_message = "Database restore failed: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_read_schema()) {
            result.error_message = "Failed to read table schema: " +
                                   conn_->last_error();
            return result;
        }

        result.schema = schema_;
        result.rows_read = step_extract_rows(callback);
        result.success = true;

        LOG_INFO("Restore extraction complete: %llu rows",
                 (unsigned long long)result.rows_read);

    } catch (const BakReadError& e) {
        result.error_message = e.what();
        LOG_ERROR("Restore extraction failed: %s", e.what());
    } catch (const std::exception& e) {
        result.error_message = std::string("Unexpected error: ") + e.what();
        LOG_ERROR("Restore extraction failed: %s", e.what());
    }

    step_cleanup();
    return result;
}

RestoreListTablesResult RestoreAdapter::list_tables() {
    RestoreListTablesResult result;

    try {
        LOG_INFO("=== Restore Mode - List Tables ===");

        if (!step_connect()) {
            result.error_message = "Failed to connect to SQL Server: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_read_backup_info()) {
            result.error_message = "Failed to read backup metadata: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_provision_tde()) {
            result.error_message = "TDE certificate provisioning failed: " +
                                   conn_->last_error();
            return result;
        }

        if (!step_restore_database()) {
            result.error_message = "Database restore failed: " +
                                   conn_->last_error();
            return result;
        }

        // Query user tables from the restored database
        std::string sql = "USE [" + temp_db_name_ + "]; "
            "SELECT s.name + '.' + t.name AS full_name "
            "FROM sys.tables t "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE t.type = 'U' "
            "ORDER BY s.name, t.name";

        if (!conn_->execute(sql)) {
            result.error_message = "Failed to query tables: " + conn_->last_error();
            return result;
        }

        SQLRETURN ret;
        while ((ret = SQLFetch(conn_->raw_stmt())) == SQL_SUCCESS ||
               ret == SQL_SUCCESS_WITH_INFO) {
            char buf[512];
            SQLLEN ind;
            SQLGetData(conn_->raw_stmt(), 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ind != SQL_NULL_DATA) {
                result.tables.push_back(buf);
            }
        }
        SQLFreeStmt(conn_->raw_stmt(), SQL_CLOSE);

        result.success = !result.tables.empty();
        if (!result.success && result.error_message.empty()) {
            result.error_message = "No user tables found in database";
        }

        LOG_INFO("Found %zu tables via restore mode", result.tables.size());

    } catch (const BakReadError& e) {
        result.error_message = e.what();
        LOG_ERROR("List tables failed: %s", e.what());
    } catch (const std::exception& e) {
        result.error_message = std::string("Unexpected error: ") + e.what();
        LOG_ERROR("List tables failed: %s", e.what());
    }

    step_cleanup();
    return result;
}

bool RestoreAdapter::step_connect() {
    LOG_INFO("Step 1: Connecting to SQL Server '%s'...",
             opts_.target_server.c_str());

    // Check for credentials in environment variables if not provided
    std::string username = opts_.sql_username;
    std::string password = opts_.sql_password;
    
    if (username.empty()) {
        const char* env_user = std::getenv("BAKREAD_SQL_USER");
        if (env_user) username = env_user;
    }
    if (password.empty()) {
        const char* env_pass = std::getenv("BAKREAD_SQL_PASSWORD");
        if (env_pass) password = env_pass;
    }

    conn_ = std::make_unique<OdbcConnection>();
    return conn_->connect(opts_.target_server, "master", username, password);
}

bool RestoreAdapter::step_read_backup_info() {
    LOG_INFO("Step 2: Reading backup metadata...");

    // RESTORE HEADERONLY
    std::string sql = "RESTORE HEADERONLY FROM " + build_from_disk_clause();
    if (!conn_->execute(sql)) {
        LOG_ERROR("RESTORE HEADERONLY failed");
        return false;
    }

    // Read header results
    SQLRETURN ret;
    while ((ret = SQLFetch(conn_->raw_stmt())) == SQL_SUCCESS) {
        BackupSetInfo bsi;

        char buf[1024];
        SQLLEN ind;

        SQLGetData(conn_->raw_stmt(), 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
        // Field 1 = BackupName

        SQLGetData(conn_->raw_stmt(), 4, SQL_C_CHAR, buf, sizeof(buf), &ind);
        if (ind != SQL_NULL_DATA) bsi.database_name = buf;

        SQLGetData(conn_->raw_stmt(), 7, SQL_C_CHAR, buf, sizeof(buf), &ind);
        if (ind != SQL_NULL_DATA) bsi.server_name = buf;

        int32_t bt = 0;
        SQLGetData(conn_->raw_stmt(), 3, SQL_C_SLONG, &bt, sizeof(bt), &ind);
        bsi.backup_type = static_cast<BackupType>(bt);

        int32_t pos = 0;
        SQLGetData(conn_->raw_stmt(), 2, SQL_C_SLONG, &pos, sizeof(pos), &ind);
        bsi.position = pos;

        int32_t compressed = 0;
        SQLGetData(conn_->raw_stmt(), 53, SQL_C_SLONG, &compressed, sizeof(compressed), &ind);
        bsi.is_compressed = (compressed != 0);

        // Check for encryption (column varies by version)
        int32_t encrypted = 0;
        SQLGetData(conn_->raw_stmt(), 62, SQL_C_SLONG, &encrypted, sizeof(encrypted), &ind);
        bsi.is_encrypted = (ind != SQL_NULL_DATA && encrypted != 0);

        backup_info_.backup_sets.push_back(bsi);

        LOG_INFO("  Backup set %d: DB='%s' Type=%d Compressed=%s Encrypted=%s",
                 bsi.position, bsi.database_name.c_str(),
                 static_cast<int>(bsi.backup_type),
                 bsi.is_compressed ? "yes" : "no",
                 bsi.is_encrypted ? "yes" : "no");
    }
    SQLFreeStmt(conn_->raw_stmt(), SQL_CLOSE);

    if (backup_info_.backup_sets.empty()) {
        LOG_ERROR("No backup sets found in file");
        return false;
    }

    // RESTORE FILELISTONLY
    sql = "RESTORE FILELISTONLY FROM " + build_from_disk_clause();
    if (!conn_->execute(sql)) {
        LOG_WARN("RESTORE FILELISTONLY failed -- will use defaults for file relocation");
    } else {
        while ((ret = SQLFetch(conn_->raw_stmt())) == SQL_SUCCESS) {
            BackupFileInfo bfi;
            char buf[1024];
            SQLLEN ind;

            SQLGetData(conn_->raw_stmt(), 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ind != SQL_NULL_DATA) bfi.logical_name = buf;

            SQLGetData(conn_->raw_stmt(), 2, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ind != SQL_NULL_DATA) bfi.physical_name = buf;

            SQLGetData(conn_->raw_stmt(), 3, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ind != SQL_NULL_DATA && ind > 0) bfi.file_type = buf[0];

            int64_t sz = 0;
            SQLGetData(conn_->raw_stmt(), 4, SQL_C_SBIGINT, &sz, sizeof(sz), &ind);
            bfi.size = sz;

            backup_info_.file_list.push_back(bfi);

            LOG_INFO("  File: '%s' (%c, %lld bytes)",
                     bfi.logical_name.c_str(), bfi.file_type,
                     (long long)bfi.size);
        }
        SQLFreeStmt(conn_->raw_stmt(), SQL_CLOSE);
    }

    return true;
}

bool RestoreAdapter::step_provision_tde() {
    LOG_INFO("Step 3: Checking TDE requirements...");

    if (opts_.tde_cert_pfx.empty()) {
        LOG_DEBUG("No TDE certificate specified -- skipping provisioning");
        return true;
    }

    LOG_INFO("Provisioning TDE certificate from: %s", opts_.tde_cert_pfx.c_str());

    // Ensure master key exists
    std::string mk_check;
    conn_->query_scalar(
        "SELECT CASE WHEN EXISTS(SELECT 1 FROM sys.symmetric_keys "
        "WHERE name = '##MS_DatabaseMasterKey##') THEN 'Y' ELSE 'N' END",
        mk_check);

    if (mk_check != "Y") {
        std::string mk_pass = opts_.master_key_password;
        if (mk_pass.empty()) mk_pass = "BakRead_TempMK_#2024!";

        std::string sql = "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'" +
                          mk_pass + "'";
        if (!conn_->execute(sql)) {
            LOG_ERROR("Failed to create master key");
            return false;
        }
        master_key_created_ = true;
        LOG_INFO("Master key created in master database");
    }

    // Import the certificate
    std::string cert_pass = opts_.tde_cert_password;
    if (cert_pass.empty()) {
        // Check environment variable
        const char* env_pass = std::getenv("BAKREAD_TDE_PASSWORD");
        if (env_pass) cert_pass = env_pass;
    }

    if (cert_pass.empty()) {
        LOG_ERROR("TDE certificate password required. Use --tde-cert-password "
                  "or set BAKREAD_TDE_PASSWORD environment variable.");
        return false;
    }

    std::string cert_name = "bakread_tde_cert_" + temp_db_name_;
    std::string sql;
    
    // Determine the private key file path
    std::string key_file = opts_.tde_cert_key.empty() 
                         ? opts_.tde_cert_pfx   // PFX format: same file
                         : opts_.tde_cert_key;  // Separate .pvk file
    
    // Try with separate cert and key files first (SQL Server native format)
    sql = "CREATE CERTIFICATE [" + cert_name + "] "
          "FROM FILE = N'" + opts_.tde_cert_pfx + "' "
          "WITH PRIVATE KEY (FILE = N'" + key_file + "', "
          "DECRYPTION BY PASSWORD = N'" + cert_pass + "')";

    if (!conn_->execute(sql)) {
        // Try PFX format (combined cert + key in one file)
        sql = "CREATE CERTIFICATE [" + cert_name + "] "
              "FROM FILE = N'" + opts_.tde_cert_pfx + "' "
              "WITH PRIVATE KEY (DECRYPTION BY PASSWORD = N'" + cert_pass + "')";
        if (!conn_->execute(sql)) {
            LOG_ERROR("Failed to import TDE certificate. Ensure the certificate "
                      "file exists and the password is correct.");
            return false;
        }
    }

    cert_imported_ = true;
    LOG_INFO("TDE certificate imported: %s", cert_name.c_str());
    return true;
}

bool RestoreAdapter::step_restore_database() {
    LOG_INFO("Step 4: Restoring database as '%s'...", temp_db_name_.c_str());

    // Get default data/log directories
    std::string default_data_dir, default_log_dir;
    conn_->query_scalar(
        "SELECT CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(256))",
        default_data_dir);
    conn_->query_scalar(
        "SELECT CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR(256))",
        default_log_dir);

    if (default_data_dir.empty()) default_data_dir = "C:\\SQLData\\";
    if (default_log_dir.empty()) default_log_dir = default_data_dir;

    // Build RESTORE command with MOVE clauses
    std::ostringstream sql;
    sql << "RESTORE DATABASE [" << temp_db_name_ << "] "
        << "FROM " << build_from_disk_clause() << " "
        << "WITH ";

    // Add MOVE clauses for each file
    for (size_t i = 0; i < backup_info_.file_list.size(); ++i) {
        auto& fi = backup_info_.file_list[i];
        std::string target_dir = (fi.file_type == 'L') ? default_log_dir : default_data_dir;
        std::string ext = (fi.file_type == 'L') ? ".ldf" : ".mdf";
        if (i > 0 && fi.file_type != 'L') ext = ".ndf";

        sql << "MOVE N'" << fi.logical_name << "' TO N'"
            << target_dir << temp_db_name_ << "_" << i << ext << "'";

        if (i < backup_info_.file_list.size() - 1) sql << ", ";
    }

    // If no file list was obtained, try without MOVE (risky but may work)
    if (backup_info_.file_list.empty()) {
        sql.str("");
        sql << "RESTORE DATABASE [" << temp_db_name_ << "] "
            << "FROM " << build_from_disk_clause() << " "
            << "WITH ";
    }

    sql << ", REPLACE, RECOVERY";

    // Backup set selection
    if (opts_.backupset > 0) {
        sql << ", FILE = " << opts_.backupset;
    }

    sql << ", STATS = 10";

    LOG_DEBUG("RESTORE SQL: %s", sql.str().c_str());

    // RESTORE DATABASE returns multiple result sets with progress info.
    // We must consume all of them before using the connection again.
    if (!conn_->execute(sql.str(), true /* consume_results */)) {
        LOG_ERROR("RESTORE DATABASE failed: %s", conn_->last_error().c_str());
        return false;
    }

    db_restored_ = true;
    LOG_INFO("Database restored successfully: %s", temp_db_name_.c_str());

    // Wait for database to become ONLINE (TDE databases may need extra time)
    for (int attempt = 0; attempt < 30; ++attempt) {
        std::string state;
        conn_->query_scalar(
            "SELECT state_desc FROM sys.databases WHERE name = N'" +
            temp_db_name_ + "'",
            state);

        if (state == "ONLINE") {
            LOG_DEBUG("Database is ONLINE after %d check(s)", attempt + 1);
            break;
        }

        LOG_DEBUG("Database state: %s, waiting... (attempt %d)", state.c_str(), attempt + 1);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // Verify TDE state if applicable
    if (!opts_.tde_cert_pfx.empty()) {
        std::string tde_state;
        conn_->query_scalar(
            "SELECT encryption_state FROM " + temp_db_name_ +
            ".sys.dm_database_encryption_keys",
            tde_state);
        LOG_INFO("TDE encryption state: %s", tde_state.c_str());
    }

    return true;
}

bool RestoreAdapter::step_read_schema() {
    LOG_INFO("Step 5: Reading table schema for '%s.%s'...",
             opts_.schema_name.c_str(), opts_.table_name.c_str());

    // Switch to the restored database
    conn_->execute("USE [" + temp_db_name_ + "]");

    // Verify table exists
    std::string exists_check;
    conn_->query_scalar(
        "SELECT CASE WHEN OBJECT_ID(N'" + opts_.schema_name + "." +
        opts_.table_name + "', N'U') IS NOT NULL THEN 'Y' ELSE 'N' END",
        exists_check);

    if (exists_check != "Y") {
        LOG_ERROR("Table '%s.%s' not found in restored database",
                  opts_.schema_name.c_str(), opts_.table_name.c_str());

        // List available tables
        LOG_INFO("Available user tables:");
        conn_->execute(
            "SELECT s.name + '.' + t.name FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "ORDER BY s.name, t.name");
        SQLRETURN ret;
        while ((ret = SQLFetch(conn_->raw_stmt())) == SQL_SUCCESS) {
            char buf[256];
            SQLLEN ind;
            SQLGetData(conn_->raw_stmt(), 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ind != SQL_NULL_DATA) LOG_INFO("  %s", buf);
        }
        SQLFreeStmt(conn_->raw_stmt(), SQL_CLOSE);

        throw TableNotFoundError(opts_.schema_name, opts_.table_name);
    }

    // Get object_id
    int64_t obj_id = 0;
    conn_->query_scalar_int(
        "SELECT OBJECT_ID(N'" + opts_.schema_name + "." + opts_.table_name + "')",
        obj_id);

    schema_.object_id   = static_cast<int32_t>(obj_id);
    schema_.schema_name = opts_.schema_name;
    schema_.table_name  = opts_.table_name;

    // Get column definitions
    std::string col_query =
        "SELECT c.column_id, c.name, t.name AS type_name, "
        "       c.system_type_id, c.max_length, c.precision, c.scale, "
        "       c.is_nullable, c.is_identity, c.is_computed "
        "FROM sys.columns c "
        "JOIN sys.types t ON c.user_type_id = t.user_type_id "
        "WHERE c.object_id = " + std::to_string(obj_id) + " "
        "ORDER BY c.column_id";

    conn_->execute(col_query);
    SQLRETURN ret;
    while ((ret = SQLFetch(conn_->raw_stmt())) == SQL_SUCCESS) {
        ColumnDef col;
        SQLLEN ind;

        int32_t col_id;
        SQLGetData(conn_->raw_stmt(), 1, SQL_C_SLONG, &col_id, sizeof(col_id), &ind);
        col.column_id = col_id;

        char name_buf[256];
        SQLGetData(conn_->raw_stmt(), 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &ind);
        col.name = name_buf;

        int32_t sys_type;
        SQLGetData(conn_->raw_stmt(), 4, SQL_C_SLONG, &sys_type, sizeof(sys_type), &ind);
        col.type = static_cast<SqlType>(sys_type);

        int16_t max_len;
        SQLGetData(conn_->raw_stmt(), 5, SQL_C_SSHORT, &max_len, sizeof(max_len), &ind);
        col.max_length = max_len;

        uint8_t prec;
        SQLGetData(conn_->raw_stmt(), 6, SQL_C_UTINYINT, &prec, sizeof(prec), &ind);
        col.precision = prec;

        uint8_t sc;
        SQLGetData(conn_->raw_stmt(), 7, SQL_C_UTINYINT, &sc, sizeof(sc), &ind);
        col.scale = sc;

        int32_t nullable;
        SQLGetData(conn_->raw_stmt(), 8, SQL_C_SLONG, &nullable, sizeof(nullable), &ind);
        col.is_nullable = (nullable != 0);

        int32_t identity;
        SQLGetData(conn_->raw_stmt(), 9, SQL_C_SLONG, &identity, sizeof(identity), &ind);
        col.is_identity = (identity != 0);

        int32_t computed;
        SQLGetData(conn_->raw_stmt(), 10, SQL_C_SLONG, &computed, sizeof(computed), &ind);
        col.is_computed = (computed != 0);

        schema_.columns.push_back(col);
        LOG_DEBUG("  Column: %s (type=%d, len=%d)", col.name.c_str(),
                  static_cast<int>(col.type), col.max_length);
    }
    SQLFreeStmt(conn_->raw_stmt(), SQL_CLOSE);

    // Filter columns if user specified a subset
    if (!opts_.columns.empty()) {
        std::vector<ColumnDef> filtered;
        for (auto& req : opts_.columns) {
            for (auto& c : schema_.columns) {
                if (c.name == req) { filtered.push_back(c); break; }
            }
        }
        if (!filtered.empty()) schema_.columns = std::move(filtered);
    }

    // Check index type
    int64_t has_clustered = 0;
    conn_->query_scalar_int(
        "SELECT COUNT(*) FROM sys.indexes "
        "WHERE object_id = " + std::to_string(obj_id) + " AND type = 1",
        has_clustered);
    schema_.is_heap = (has_clustered == 0);

    LOG_INFO("Table schema: %s (%zu columns, %s)",
             schema_.qualified_name().c_str(), schema_.columns.size(),
             schema_.is_heap ? "heap" : "clustered");

    return true;
}

uint64_t RestoreAdapter::step_extract_rows(RowCallback& callback) {
    LOG_INFO("Step 6: Extracting rows...");

    std::string query = build_select_query();
    LOG_INFO("Query: %s", query.c_str());

    uint64_t count = 0;
    auto counting_callback = [&](const Row& row) -> bool {
        ++count;
        if (count % 100000 == 0)
            LOG_INFO("  Exported %llu rows...", (unsigned long long)count);
        return callback(row);
    };

    conn_->query_rows(query, schema_.columns, counting_callback, opts_.max_rows);
    return count;
}

void RestoreAdapter::step_cleanup() {
    if (!conn_ || !conn_->is_connected()) return;

    LOG_INFO("Step 7: Cleanup...");

    // Switch back to master
    conn_->execute("USE master");

    // Drop temporary database
    if (db_restored_) {
        LOG_INFO("Dropping temporary database: %s", temp_db_name_.c_str());
        conn_->execute("ALTER DATABASE [" + temp_db_name_ +
                        "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
        conn_->execute("DROP DATABASE [" + temp_db_name_ + "]");
        db_restored_ = false;
    }

    // Cleanup certificates if requested
    if (cert_imported_ && opts_.cleanup_keys) {
        std::string cert_name = "bakread_tde_cert_" + temp_db_name_;
        LOG_INFO("Removing certificate: %s", cert_name.c_str());
        conn_->execute("DROP CERTIFICATE [" + cert_name + "]");
        cert_imported_ = false;
    }

    if (master_key_created_ && opts_.cleanup_keys) {
        LOG_INFO("Dropping temporary master key");
        conn_->execute("DROP MASTER KEY");
        master_key_created_ = false;
    }
}

std::string RestoreAdapter::build_select_query() const {
    std::ostringstream sql;
    sql << "SELECT ";

    if (opts_.max_rows > 0) {
        sql << "TOP(" << opts_.max_rows << ") ";
    }

    if (schema_.columns.empty()) {
        sql << "*";
    } else {
        for (size_t i = 0; i < schema_.columns.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << "[" << schema_.columns[i].name << "]";
        }
    }

    sql << " FROM [" << opts_.schema_name << "].[" << opts_.table_name << "]";

    if (!opts_.where_clause.empty()) {
        sql << " WHERE " << opts_.where_clause;
    }

    return sql.str();
}

std::string RestoreAdapter::build_from_disk_clause() const {
    std::ostringstream ss;
    for (size_t i = 0; i < opts_.bak_paths.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << "DISK = N'" << opts_.bak_paths[i] << "'";
    }
    return ss.str();
}

std::string RestoreAdapter::generate_temp_db_name() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

    std::mt19937 rng(static_cast<unsigned>(ms));
    std::uniform_int_distribution<int> dist(1000, 9999);

    return "bakread_tmp_" + std::to_string(ms % 100000) + "_" +
           std::to_string(dist(rng));
}

}  // namespace bakread
