#include "bakread/backup_header.h"
#include "bakread/backup_stream.h"
#include "bakread/cli.h"
#include "bakread/direct_extractor.h"
#include "bakread/error.h"
#include "bakread/logging.h"
#include "bakread/pipeline.h"
#include "bakread/restore_adapter.h"

#include <cstdlib>
#include <iostream>
#include <iomanip>

int main(int argc, char* argv[]) {
    using namespace bakread;

    if (argc < 2) {
        print_usage();
        return 1;
    }

    try {
        Options opts = parse_args(argc, argv);

        auto& log = Logger::instance();
        if (opts.verbose) log.set_verbose(true);
        if (!opts.log_file.empty()) log.set_log_file(opts.log_file);

        if (opts.print_data_offset) {
            BackupStream stream(opts.bak_paths[0]);
            BackupHeaderParser parser(stream);
            if (!parser.parse()) {
                std::cerr << "Failed to parse backup header\n";
                return 1;
            }
            std::cout << "data_start_offset=" << parser.data_start_offset() << "\n";
            if (!parser.backup_sets().empty()) {
                const auto& bs = parser.backup_sets()[0];
                std::cout << "database_name=" << bs.database_name << "\n";
                std::cout << "backup_type=" << static_cast<int>(bs.backup_type)
                          << " is_compressed=" << (bs.is_compressed ? 1 : 0) << "\n";
            }
            return 0;
        }

        if (opts.list_tables) {
            LOG_INFO("========================================");
            LOG_INFO("bakread - List Tables Mode");
            LOG_INFO("========================================");
            
            if (opts.bak_paths.empty()) {
                std::cerr << "Error: No backup file specified.\n";
                return 1;
            }
            
            // If target server is specified, use restore mode directly (more reliable)
            if (!opts.target_server.empty()) {
                LOG_INFO("Using restore mode to list tables...");
                
                RestoreOptions ropts;
                ropts.bak_paths = opts.bak_paths;
                ropts.target_server = opts.target_server;
                ropts.sql_username = opts.sql_username;
                ropts.sql_password = opts.sql_password;
                ropts.tde_cert_pfx = opts.tde_cert_pfx;
                ropts.tde_cert_key = opts.tde_cert_key;
                ropts.tde_cert_password = opts.tde_cert_password;
                
                RestoreAdapter adapter(ropts);
                auto restore_result = adapter.list_tables();
                
                if (restore_result.success && !restore_result.tables.empty()) {
                    std::cout << "\n";
                    std::cout << std::left << std::setw(50) << "TABLE NAME" << "\n";
                    std::cout << std::string(50, '-') << "\n";
                    
                    for (const auto& tbl : restore_result.tables) {
                        std::cout << std::left << std::setw(50) << tbl << "\n";
                    }
                    std::cout << "\nFound " << restore_result.tables.size() << " table(s).\n";
                    return 0;
                }
                
                if (!restore_result.error_message.empty()) {
                    std::cerr << "Error: " << restore_result.error_message << "\n";
                } else {
                    std::cerr << "No tables found in backup.\n";
                }
                return 1;
            }
            
            // No target server - try direct mode
            LOG_INFO("Scanning backup for tables (direct mode)...");
            
            // Configure extractor with indexed mode if requested
            DirectExtractorConfig config;
            config.use_indexed_mode = opts.indexed_mode;
            config.cache_size_mb = opts.cache_size_mb;
            config.index_dir = opts.index_dir;
            config.force_rescan = opts.force_rescan;
            
            DirectExtractor extractor(opts.bak_paths, config);
            auto result = extractor.list_tables();
            
            if (result.success && !result.tables.empty()) {
                std::cout << "\n";
                std::cout << std::left << std::setw(30) << "TABLE NAME" 
                          << std::setw(12) << "ROWS (est)" 
                          << std::setw(15) << "DATA PAGES" << "\n";
                std::cout << std::string(57, '-') << "\n";
                
                for (const auto& tbl : result.tables) {
                    std::cout << std::left << std::setw(30) << tbl.full_name
                              << std::setw(12) << (tbl.row_count >= 0 ? std::to_string(tbl.row_count) : "?")
                              << std::setw(15) << (tbl.page_count >= 0 ? std::to_string(tbl.page_count) : "?")
                              << "\n";
                }
                std::cout << "\nFound " << result.tables.size() << " table(s).\n";
                return 0;
            }
            
            if (result.error_message.empty()) {
                std::cerr << "No tables found in backup.\n";
            } else {
                std::cerr << "Error: " << result.error_message << "\n";
            }
            
            std::cerr << "\nTip: For TDE-encrypted or compressed backups, use --target-server to list tables via restore mode.\n";
            return 1;
        }

        Pipeline pipeline(opts);
        auto result = pipeline.run();

        return result.success ? 0 : 1;

    } catch (const ConfigError& e) {
        std::cerr << "Configuration error: " << e.what() << "\n";
        std::cerr << "Run 'bakread --help' for usage information.\n";
        return 2;
    } catch (const BakReadError& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 3;
    }
}
