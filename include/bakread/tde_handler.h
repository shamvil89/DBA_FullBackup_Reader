#pragma once

#include "bakread/types.h"

#include <string>

namespace bakread {

class OdbcConnection;

// -------------------------------------------------------------------------
// TdeHandler -- manages TDE certificate detection, export, and import
//
// This module handles the complex workflow of:
//   1. Detecting if a database backup uses TDE
//   2. Identifying which certificate protects the DEK
//   3. Exporting the certificate from a source SQL Server
//   4. Importing it into the target SQL Server
//   5. Ensuring the master key exists
//   6. Cleanup after extraction
//
// CRITICAL CONSTRAINT:
//   This tool does NOT claim it can recover or derive TDE keys from a
//   .bak file. Keys must be provided externally or sourced from
//   SQL Server via authorized access.
// -------------------------------------------------------------------------

struct TdeDetectionResult {
    bool is_tde_enabled     = false;
    bool is_backup_encrypted = false;
    std::string cert_name;
    std::string key_algorithm;
    int32_t     encryption_state = 0;
    std::string error;
};

struct TdeCertExportResult {
    bool success = false;
    std::string cert_file_path;
    std::string key_file_path;
    std::string error;
};

class TdeHandler {
public:
    // Detect TDE status on a live database
    static TdeDetectionResult detect_tde(OdbcConnection& conn,
                                          const std::string& database_name);

    // Export TDE certificate from a source server
    // Requires sysadmin or sufficient permissions on the source
    static TdeCertExportResult export_certificate(
        OdbcConnection& source_conn,
        const std::string& database_name,
        const std::string& export_path,
        const std::string& export_password);

    // Import TDE certificate into a target server
    static bool import_certificate(
        OdbcConnection& target_conn,
        const std::string& cert_file,
        const std::string& key_file,
        const std::string& password,
        const std::string& cert_name);

    // Ensure a database master key exists in the master database
    static bool ensure_master_key(OdbcConnection& conn,
                                   const std::string& password);

    // Remove an imported certificate
    static bool remove_certificate(OdbcConnection& conn,
                                    const std::string& cert_name);

    // Check if EKM/HSM is in use (non-exportable keys)
    static bool is_ekm_protected(OdbcConnection& conn,
                                  const std::string& database_name);
};

}  // namespace bakread
