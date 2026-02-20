#include "bakread/cli.h"
#include "bakread/error.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>

namespace bakread {

static std::string next_arg(int& i, int argc, char* argv[], const char* flag) {
    if (i + 1 >= argc) {
        throw ConfigError(std::string("Missing value for flag: ") + flag);
    }
    return argv[++i];
}

static void split_columns(const std::string& csv, std::vector<std::string>& out) {
    std::istringstream ss(csv);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        // Trim whitespace
        auto begin = tok.find_first_not_of(" \t");
        auto end   = tok.find_last_not_of(" \t");
        if (begin != std::string::npos)
            out.push_back(tok.substr(begin, end - begin + 1));
    }
}

Options parse_args(int argc, char* argv[]) {
    Options opts;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        }
        else if (arg == "--bak")                opts.bak_paths.push_back(next_arg(i, argc, argv, "--bak"));
        else if (arg == "--table")              opts.table_qualified    = next_arg(i, argc, argv, "--table");
        else if (arg == "--out")                opts.output_path        = next_arg(i, argc, argv, "--out");
        else if (arg == "--format") {
            auto v = next_arg(i, argc, argv, "--format");
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            if (v == "csv")          opts.format = OutputFormat::CSV;
            else if (v == "parquet") opts.format = OutputFormat::Parquet;
            else if (v == "jsonl" || v == "json") opts.format = OutputFormat::JSONL;
            else throw ConfigError("Unknown format: " + v + ". Expected csv|parquet|jsonl");
        }
        else if (arg == "--mode") {
            auto v = next_arg(i, argc, argv, "--mode");
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            if (v == "auto")         opts.mode = ExecMode::Auto;
            else if (v == "direct")  opts.mode = ExecMode::Direct;
            else if (v == "restore") opts.mode = ExecMode::Restore;
            else throw ConfigError("Unknown mode: " + v + ". Expected auto|direct|restore");
        }
        else if (arg == "--backupset")          opts.backupset          = std::stoi(next_arg(i, argc, argv, "--backupset"));
        else if (arg == "--columns")            split_columns(next_arg(i, argc, argv, "--columns"), opts.columns);
        else if (arg == "--where")              opts.where_clause       = next_arg(i, argc, argv, "--where");
        else if (arg == "--max-rows")           opts.max_rows           = std::stoll(next_arg(i, argc, argv, "--max-rows"));
        else if (arg == "--delimiter")          opts.delimiter          = next_arg(i, argc, argv, "--delimiter");
        else if (arg == "--verbose" || arg == "-v") opts.verbose = true;
        else if (arg == "--log")                opts.log_file           = next_arg(i, argc, argv, "--log");
        else if (arg == "--print-data-offset")  opts.print_data_offset  = true;
        else if (arg == "--list-tables")        opts.list_tables        = true;
        else if (arg == "--allocation-hint")    opts.allocation_hint_path = next_arg(i, argc, argv, "--allocation-hint");

        // Indexed mode (for large backups)
        else if (arg == "--indexed")            opts.indexed_mode = true;
        else if (arg == "--cache-size")         opts.cache_size_mb = std::stoull(next_arg(i, argc, argv, "--cache-size"));
        else if (arg == "--index-dir")          opts.index_dir = next_arg(i, argc, argv, "--index-dir");
        else if (arg == "--force-rescan")       opts.force_rescan = true;

        // SQL Server Authentication
        else if (arg == "--sql-user" || arg == "-U")
            opts.sql_username = next_arg(i, argc, argv, "--sql-user");
        else if (arg == "--sql-password" || arg == "-P")
            opts.sql_password = next_arg(i, argc, argv, "--sql-password");

        // TDE / Encryption
        else if (arg == "--tde-cert-pfx")       opts.tde_cert_pfx       = next_arg(i, argc, argv, "--tde-cert-pfx");
        else if (arg == "--tde-cert-key")       opts.tde_cert_key       = next_arg(i, argc, argv, "--tde-cert-key");
        else if (arg == "--tde-cert-password")  opts.tde_cert_password  = next_arg(i, argc, argv, "--tde-cert-password");
        else if (arg == "--backup-cert-pfx")    opts.backup_cert_pfx    = next_arg(i, argc, argv, "--backup-cert-pfx");
        else if (arg == "--source-server")      opts.source_server      = next_arg(i, argc, argv, "--source-server");
        else if (arg == "--target-server")      opts.target_server      = next_arg(i, argc, argv, "--target-server");
        else if (arg == "--master-key-password") opts.master_key_password = next_arg(i, argc, argv, "--master-key-password");
        else if (arg == "--allow-key-export-to-disk") opts.allow_key_export = true;
        else if (arg == "--cleanup-keys")       opts.cleanup_keys = true;

        else {
            throw ConfigError("Unknown argument: " + arg);
        }
    }

    opts.resolve_table_name();
    // Skip validation for special modes that don't need all options
    if (!opts.print_data_offset && !opts.list_tables)
        opts.validate();
    return opts;
}

void Options::resolve_table_name() {
    if (table_qualified.empty()) return;

    auto dot = table_qualified.find('.');
    if (dot != std::string::npos) {
        schema_name = table_qualified.substr(0, dot);
        table_name  = table_qualified.substr(dot + 1);
    } else {
        schema_name = "dbo";
        table_name  = table_qualified;
    }

    // Strip bracket notation: [dbo].[Orders] -> dbo.Orders
    auto strip = [](std::string& s) {
        if (s.size() >= 2 && s.front() == '[' && s.back() == ']') {
            s = s.substr(1, s.size() - 2);
        }
    };
    strip(schema_name);
    strip(table_name);
}

void Options::validate() const {
    if (bak_paths.empty())
        throw ConfigError("--bak is required (specify one or more backup files)");
    if (print_data_offset)
        return;
    if (table_name.empty())
        throw ConfigError("--table is required (use schema.table format)");
    if (output_path.empty())
        throw ConfigError("--out is required");
    if (mode == ExecMode::Restore && target_server.empty()) {
        // Default target will be set later if needed
    }
}

void print_usage() {
    std::cout << R"(
bakread - SQL Server .bak Backup Table Extractor

USAGE:
    bakread --bak <PATH> [--bak <PATH2> ...] --table <schema.table> --out <PATH> --format <fmt> [OPTIONS]

REQUIRED:
    --bak PATH              Path to a .bak backup file (repeat for striped backups)
    --table schema.table    Schema-qualified table name (e.g. dbo.Orders)
    --out PATH              Output file path
    --format csv|parquet|jsonl  Output format

MODE:
    --mode auto|direct|restore
        auto     Try direct parse, fallback to restore (default)
        direct   Only attempt direct .bak parsing
        restore  Restore to SQL Server, then extract via ODBC

FILTERING:
    --backupset N           Select backup set by position (default: first full)
    --columns "c1,c2,c3"    Comma-separated list of columns to export
    --where "condition"     SQL WHERE clause for filtering (restore mode only)
    --max-rows N            Maximum number of rows to export
    --delimiter ","         CSV delimiter (default: comma)
    --allocation-hint FILE  CSV file with (file_id,page_id) to filter pages (direct mode)

LOGGING:
    --verbose, -v           Enable verbose output
    --log FILE              Write log to file

SQL SERVER CONNECTION:
    --target-server SERVER        Target SQL Server for restore mode
    --sql-user, -U USER           SQL Server login (default: Windows Auth)
    --sql-password, -P PASS       SQL Server password (or set BAKREAD_SQL_PASSWORD env)

TDE / ENCRYPTION:
    --tde-cert-pfx PATH           Certificate file (.cer or .pfx)
    --tde-cert-key PATH           Private key file (.pvk) - if using separate files
    --tde-cert-password VALUE     Password for the key (or set BAKREAD_TDE_PASSWORD env)
    --backup-cert-pfx PATH        PFX for backup-level encryption
    --source-server SERVER        Source SQL Server to export TDE certs from
    --master-key-password VALUE   Database master key password
    --allow-key-export-to-disk    Allow temporary key export to disk
    --cleanup-keys                Remove imported certificates after extraction

LARGE BACKUP MODE (indexed):
    --indexed               Use indexed page store (recommended for >1GB backups)
    --cache-size MB         LRU cache size in MB (default: 256)
    --index-dir PATH        Directory for index files (default: next to backup)
    --force-rescan          Ignore existing index files and rescan

EXAMPLES:
    bakread --bak backup.bak --table dbo.Orders --out orders.csv --format csv
    bakread --bak backup.bak --table dbo.Users --out users.parquet --format parquet
    bakread --bak backup.bak --table dbo.Sales --mode restore --target-server ".\SQLEXPRESS" --out sales.jsonl --format jsonl

    # Striped backup (multiple files):
    bakread --bak stripe1.bak --bak stripe2.bak --bak stripe3.bak --table dbo.Orders --out orders.csv --format csv

    # Large backup with indexed mode (50GB StackOverflow):
    bakread --bak StackOverflow_1of4.bak --bak StackOverflow_2of4.bak --bak StackOverflow_3of4.bak --bak StackOverflow_4of4.bak \
            --table dbo.Users --out users.csv --format csv --indexed --cache-size 512

SPECIAL MODES:
    --list-tables           List all tables in the backup and exit
    --print-data-offset     Parse backup header and print data region offset

)";
}

}  // namespace bakread
