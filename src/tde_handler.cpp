#include "bakread/tde_handler.h"
#include "bakread/error.h"
#include "bakread/logging.h"
#include "bakread/restore_adapter.h"

#include <cstdlib>
#include <filesystem>

namespace bakread {

TdeDetectionResult TdeHandler::detect_tde(OdbcConnection& conn,
                                           const std::string& database_name) {
    TdeDetectionResult result;

    // Query the database encryption keys DMV
    std::string sql =
        "SELECT dek.encryption_state, c.name AS cert_name, "
        "       dek.key_algorithm, dek.key_length "
        "FROM sys.dm_database_encryption_keys dek "
        "JOIN sys.certificates c ON dek.encryptor_thumbprint = c.thumbprint "
        "WHERE dek.database_id = DB_ID(N'" + database_name + "')";

    std::string cert_name, key_algo;
    int64_t state = 0;

    if (conn.query_scalar_int(
            "SELECT encryption_state FROM sys.dm_database_encryption_keys "
            "WHERE database_id = DB_ID(N'" + database_name + "')",
            state)) {
        result.encryption_state = static_cast<int32_t>(state);
        result.is_tde_enabled = (state > 0);
    }

    if (result.is_tde_enabled) {
        conn.query_scalar(
            "SELECT c.name FROM sys.dm_database_encryption_keys dek "
            "JOIN sys.certificates c ON dek.encryptor_thumbprint = c.thumbprint "
            "WHERE dek.database_id = DB_ID(N'" + database_name + "')",
            result.cert_name);

        conn.query_scalar(
            "SELECT key_algorithm FROM sys.dm_database_encryption_keys "
            "WHERE database_id = DB_ID(N'" + database_name + "')",
            result.key_algorithm);

        LOG_INFO("TDE detected: state=%d, cert='%s', algo='%s'",
                 result.encryption_state, result.cert_name.c_str(),
                 result.key_algorithm.c_str());
    }

    return result;
}

TdeCertExportResult TdeHandler::export_certificate(
    OdbcConnection& source_conn,
    const std::string& database_name,
    const std::string& export_path,
    const std::string& export_password) {

    TdeCertExportResult result;

    // Find the TDE certificate
    auto detection = detect_tde(source_conn, database_name);
    if (!detection.is_tde_enabled) {
        result.error = "TDE is not enabled on database: " + database_name;
        return result;
    }

    if (detection.cert_name.empty()) {
        result.error = "Cannot identify TDE certificate for: " + database_name;
        return result;
    }

    // Check for EKM (non-exportable keys)
    if (is_ekm_protected(source_conn, database_name)) {
        result.error = "TDE certificate is protected by EKM/HSM and cannot "
                       "be exported. Restore must be performed on a server "
                       "with access to the EKM provider.";
        LOG_ERROR("%s", result.error.c_str());
        return result;
    }

    namespace fs = std::filesystem;
    fs::path base(export_path);
    result.cert_file_path = (base / (detection.cert_name + ".cer")).string();
    result.key_file_path  = (base / (detection.cert_name + ".pvk")).string();

    // Export certificate
    std::string sql =
        "BACKUP CERTIFICATE [" + detection.cert_name + "] "
        "TO FILE = N'" + result.cert_file_path + "' "
        "WITH PRIVATE KEY ("
        "  FILE = N'" + result.key_file_path + "', "
        "  ENCRYPTION BY PASSWORD = N'" + export_password + "'"
        ")";

    if (!source_conn.execute(sql)) {
        result.error = "Failed to export certificate: " + source_conn.last_error();
        LOG_ERROR("%s", result.error.c_str());
        return result;
    }

    result.success = true;
    LOG_INFO("TDE certificate exported: %s, %s",
             result.cert_file_path.c_str(), result.key_file_path.c_str());
    return result;
}

bool TdeHandler::import_certificate(
    OdbcConnection& target_conn,
    const std::string& cert_file,
    const std::string& key_file,
    const std::string& password,
    const std::string& cert_name) {

    std::string sql =
        "CREATE CERTIFICATE [" + cert_name + "] "
        "FROM FILE = N'" + cert_file + "' "
        "WITH PRIVATE KEY ("
        "  FILE = N'" + key_file + "', "
        "  DECRYPTION BY PASSWORD = N'" + password + "'"
        ")";

    if (!target_conn.execute(sql)) {
        LOG_ERROR("Failed to import certificate: %s",
                  target_conn.last_error().c_str());
        return false;
    }

    LOG_INFO("Certificate imported: %s", cert_name.c_str());
    return true;
}

bool TdeHandler::ensure_master_key(OdbcConnection& conn,
                                    const std::string& password) {
    // Check if master key already exists
    std::string exists;
    conn.query_scalar(
        "SELECT CASE WHEN EXISTS("
        "  SELECT 1 FROM sys.symmetric_keys "
        "  WHERE name = '##MS_DatabaseMasterKey##'"
        ") THEN 'Y' ELSE 'N' END",
        exists);

    if (exists == "Y") {
        LOG_DEBUG("Master key already exists");
        return true;
    }

    std::string mk_pass = password.empty() ? "BakRead_MK_Tmp_#2024!" : password;

    std::string sql = "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'" +
                      mk_pass + "'";

    if (!conn.execute(sql)) {
        LOG_ERROR("Failed to create master key: %s", conn.last_error().c_str());
        return false;
    }

    LOG_INFO("Master key created");
    return true;
}

bool TdeHandler::remove_certificate(OdbcConnection& conn,
                                     const std::string& cert_name) {
    return conn.execute("DROP CERTIFICATE [" + cert_name + "]");
}

bool TdeHandler::is_ekm_protected(OdbcConnection& conn,
                                   const std::string& database_name) {
    // Check if the TDE DEK uses an asymmetric key from an EKM provider
    std::string encryptor_type;
    conn.query_scalar(
        "SELECT encryptor_type FROM sys.dm_database_encryption_keys "
        "WHERE database_id = DB_ID(N'" + database_name + "')",
        encryptor_type);

    // encryptor_type = 1 means certificate, 2 means asymmetric key
    // Asymmetric keys from EKM providers are typically non-exportable
    if (encryptor_type == "2") {
        // Further check: is this key from an EKM provider?
        int64_t ekm_count = 0;
        conn.query_scalar_int(
            "SELECT COUNT(*) FROM sys.dm_database_encryption_keys dek "
            "JOIN sys.asymmetric_keys ak ON dek.encryptor_thumbprint = ak.thumbprint "
            "JOIN sys.cryptographic_providers cp ON ak.crypto_provider_id = cp.provider_id "
            "WHERE dek.database_id = DB_ID(N'" + database_name + "')",
            ekm_count);
        return ekm_count > 0;
    }

    return false;
}

}  // namespace bakread
