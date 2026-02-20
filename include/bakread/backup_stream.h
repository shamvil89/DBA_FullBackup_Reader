#pragma once

#include "bakread/types.h"

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// Microsoft Tape Format (MTF) descriptor block types used by SQL Server
// -------------------------------------------------------------------------
enum class MtfBlockType : uint32_t {
    TAPE = 0x45504154,  // "TAPE"
    SSET = 0x54455353,  // "SSET"
    VOLB = 0x424C4F56,  // "VOLB"
    DIRB = 0x42524944,  // "DIRB"
    FILE = 0x454C4946,  // "FILE"
    CFIL = 0x4C494643,  // "CFIL"
    ESPB = 0x42505345,  // "ESPB"
    ESET = 0x54455345,  // "ESET"
    EOTM = 0x4D544F45,  // "EOTM"
    SFMB = 0x424D4653,  // "SFMB"
};

// -------------------------------------------------------------------------
// MTF descriptor block common header (52 bytes)
// -------------------------------------------------------------------------
#pragma pack(push, 1)
struct MtfBlockHeader {
    uint32_t block_type;
    uint32_t block_attributes;
    uint16_t os_id;
    uint8_t  os_version_major;
    uint8_t  os_version_minor;
    uint64_t displayable_size;
    uint64_t format_logical_address;
    uint16_t reserved_for_mbc;
    uint8_t  reserved1[6];
    uint32_t control_block_id;
    uint32_t string_storage_offset;
    uint16_t string_storage_size;
    // OS-specific data follows
};
static_assert(sizeof(MtfBlockHeader) == 46, "MtfBlockHeader size mismatch");

struct MtfTapeHeader {
    MtfBlockHeader common;
    uint32_t       media_family_id;
    uint32_t       tape_attributes;
    uint16_t       media_sequence_number;
    uint16_t       password_encryption_algorithm;
    uint16_t       soft_filemark_block_size;
    uint16_t       media_based_catalog_type;
    // Media name and description follow as strings
};

struct MtfStartOfSet {
    MtfBlockHeader common;
    uint32_t       sset_attributes;
    uint16_t       password_encryption_algorithm;
    uint16_t       software_compression_algorithm;
    uint16_t       software_vendor_id;
    uint16_t       data_set_number;
    // ... more fields follow
};
#pragma pack(pop)

// -------------------------------------------------------------------------
// SQL Server backup-specific structures embedded in the MTF stream
// -------------------------------------------------------------------------
#pragma pack(push, 1)
struct SqlBackupMediaHeader {
    uint8_t  magic[4];           // Expected: specific SQL Server signature
    uint32_t version;
    uint32_t flags;
    uint32_t block_size;
    uint64_t device_size;
    // Variable-length fields follow
};
#pragma pack(pop)

// -------------------------------------------------------------------------
// BackupStream -- streaming reader for .bak files
// -------------------------------------------------------------------------
class BackupStream {
public:
    explicit BackupStream(const std::string& path, size_t buffer_size = 4 * 1024 * 1024);
    ~BackupStream();

    BackupStream(const BackupStream&) = delete;
    BackupStream& operator=(const BackupStream&) = delete;

    // Stream position
    uint64_t position() const;
    uint64_t file_size() const;
    bool     eof() const;

    // Reading
    size_t read(void* dest, size_t count);
    bool   read_exact(void* dest, size_t count);
    bool   skip(uint64_t count);
    bool   seek(uint64_t offset);

    // Peek at data without consuming
    bool   peek(void* dest, size_t count);

    // Read a complete MTF block header
    bool   read_block_header(MtfBlockHeader& hdr);

    // Read raw bytes into a vector
    std::vector<uint8_t> read_bytes(size_t count);

    // Progress
    double progress_pct() const;

private:
    void refill();

    std::ifstream file_;
    uint64_t      file_size_   = 0;
    uint64_t      logical_pos_ = 0;

    std::vector<uint8_t> buffer_;
    size_t               buf_pos_  = 0;
    size_t               buf_len_  = 0;
};

}  // namespace bakread
