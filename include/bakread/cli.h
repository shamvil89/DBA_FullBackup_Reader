#pragma once

#include "bakread/types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace bakread {

struct Options {
    // Required (one or more .bak files for striped backups)
    std::vector<std::string> bak_paths;
    std::string  table_qualified;   // "schema.table"
    std::string  output_path;
    OutputFormat format = OutputFormat::CSV;

    // Parsed from table_qualified
    std::string  schema_name = "dbo";
    std::string  table_name;

    // Mode
    ExecMode     mode = ExecMode::Auto;

    // Optional
    int32_t      backupset     = -1;  // -1 = auto (first full)
    std::vector<std::string> columns;
    std::string  where_clause;
    int64_t      max_rows      = -1;  // -1 = unlimited
    std::string  delimiter     = ",";

    // Logging
    bool         verbose       = false;
    std::string  log_file;

    // Special modes
    bool         print_data_offset = false;  // Parse backup and print data region offset, then exit
    bool         list_tables = false;        // List all tables in the backup, then exit

    // Allocation hint: CSV file with (file_id, page_id) to filter pages
    std::string  allocation_hint_path;

    // Indexed mode (for large backups)
    bool         indexed_mode = false;       // Use indexed page store instead of in-memory
    size_t       cache_size_mb = 256;        // LRU cache size in MB (default 256MB = 32K pages)
    std::string  index_dir;                  // Directory for index files (empty = auto)
    bool         force_rescan = false;       // Ignore existing index files

    // SQL Server Authentication
    std::string  sql_username;       // SQL login (if not using Windows Auth)
    std::string  sql_password;       // SQL password

    // TDE / Encryption
    std::string  tde_cert_pfx;       // PFX file OR .cer file
    std::string  tde_cert_key;       // .pvk file (if using separate cert/key)
    std::string  tde_cert_password;
    std::string  backup_cert_pfx;
    std::string  source_server;
    std::string  target_server;
    std::string  master_key_password;
    bool         allow_key_export = false;
    bool         cleanup_keys     = false;

    // Validate options and set defaults
    void validate() const;

    // Parse schema.table into schema_name / table_name
    void resolve_table_name();
};

Options parse_args(int argc, char* argv[]);
void    print_usage();

}  // namespace bakread
