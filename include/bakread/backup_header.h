#pragma once

#include "bakread/backup_stream.h"
#include "bakread/types.h"

#include <memory>
#include <string>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// SQL Server backup header parser
//
// Reads the MTF-based backup stream and extracts:
//   - Media header info
//   - Backup set metadata (database name, type, version, compression, TDE)
//   - File list (logical names, sizes, types)
//
// This information is equivalent to:
//   RESTORE HEADERONLY FROM DISK = 'backup.bak'
//   RESTORE FILELISTONLY FROM DISK = 'backup.bak'
// -------------------------------------------------------------------------
class BackupHeaderParser {
public:
    explicit BackupHeaderParser(BackupStream& stream);

    // Parse backup header metadata -- must be called first
    bool parse();

    // Accessors
    const BackupInfo&                 info()        const { return info_; }
    const std::vector<BackupSetInfo>& backup_sets() const { return info_.backup_sets; }
    const std::vector<BackupFileInfo>& file_list()  const { return info_.file_list; }

    // Select a backup set for extraction (0-based index)
    const BackupSetInfo* select_backup_set(int index) const;

    // Detect TDE or backup encryption
    bool is_tde_enabled()     const;
    bool is_backup_encrypted() const;

    // The offset in the stream where page data begins for the selected set
    uint64_t data_start_offset() const { return data_start_offset_; }

    // SQL Server version from backup
    int32_t sql_version_major() const;

private:
    bool parse_mtf_tape_header();
    bool parse_mtf_sset();
    void parse_sset_block(const std::vector<uint8_t>& data);
    bool parse_sql_media_header(const std::vector<uint8_t>& data);
    bool parse_sql_backup_header(const std::vector<uint8_t>& data);
    bool parse_sql_file_list(const std::vector<uint8_t>& data);
    bool scan_for_data_start();

    // Read a UTF-16LE string from a data buffer
    static std::string read_utf16_string(const uint8_t* data, size_t byte_len);

    BackupStream& stream_;
    BackupInfo    info_;
    uint64_t      data_start_offset_ = 0;
};

}  // namespace bakread
