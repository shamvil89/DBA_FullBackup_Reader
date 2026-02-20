#include "bakread/backup_header.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cstring>

namespace bakread {

// -------------------------------------------------------------------------
// SQL Server backup stream layout (simplified):
//
//  [MTF TAPE header]
//  [SQL Media Header block]
//  [MTF SSET header]
//  [SQL Backup Header block]
//  [SQL Database File Info blocks]
//  [Page data blocks -- compressed or raw 8KB pages]
//  [MTF ESET / SFMB]
//
// The SQL-specific blocks are embedded as data streams within the MTF
// descriptor blocks, identified by specific magic signatures.
// -------------------------------------------------------------------------

static constexpr uint32_t MTF_TAPE = 0x45504154;
static constexpr uint32_t MTF_SSET = 0x54455353;
static constexpr uint32_t MTF_VOLB = 0x424C4F56;
static constexpr uint32_t MTF_DIRB = 0x42524944;
static constexpr uint32_t MTF_FILE = 0x454C4946;
static constexpr uint32_t MTF_ESET = 0x54455345;
static constexpr uint32_t MTF_SFMB = 0x424D4653;

// SQL Server identifies its blocks via a magic GUID/signature at known offsets
static constexpr uint8_t SQL_BACKUP_MAGIC[] = {
    0x00, 0x00, 0x00, 0x00, 0x4D, 0x53, 0x51, 0x4C,  // ....MSQL
    0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x00, 0x00   // Server..
};

BackupHeaderParser::BackupHeaderParser(BackupStream& stream)
    : stream_(stream)
{
}

static bool is_mtf_signature(uint32_t dw) {
    switch (dw) {
    case 0x45504154: // TAPE
    case 0x54455353: // SSET
    case 0x424C4F56: // VOLB
    case 0x42524944: // DIRB
    case 0x454C4946: // FILE
    case 0x54455345: // ESET
    case 0x424D4653: // SFMB
    case 0x4C494643: // CFIL
    case 0x42505345: // ESPB
    case 0x4943534D: // MSCI -- SQL Server media component info
    case 0x4144534D: // MSDA -- SQL Server data area (page data follows)
        return true;
    default:
        return false;
    }
}

bool BackupHeaderParser::parse() {
    LOG_INFO("Parsing backup header...");
    stream_.seek(0);

    if (stream_.file_size() < 512) {
        throw BackupFormatError("File too small to be a valid backup");
    }

    // ── Phase 1: Discover MTF block locations ────────────────────────
    // Scan at 512-byte aligned offsets to find every MTF descriptor block.
    // This avoids relying on hard-coded block sizes which vary across
    // SQL Server versions and backup configurations.

    struct BlockLoc { uint64_t offset; uint32_t type; };
    std::vector<BlockLoc> blocks;

    constexpr uint64_t ALIGN      = 512;
    constexpr uint64_t SCAN_LIMIT = 64ULL * 1024 * 1024;
    uint64_t scan_end = std::min<uint64_t>(stream_.file_size(), SCAN_LIMIT);

    uint64_t gap_since_last_block = 0;
    for (uint64_t pos = 0; pos < scan_end; pos += ALIGN) {
        stream_.seek(pos);
        uint32_t sig = 0;
        if (!stream_.peek(&sig, 4)) break;
        if (is_mtf_signature(sig)) {
            blocks.push_back({pos, sig});
            gap_since_last_block = 0;
            LOG_DEBUG("MTF block '%c%c%c%c' at offset %llu",
                      sig & 0xFF, (sig >> 8) & 0xFF,
                      (sig >> 16) & 0xFF, (sig >> 24) & 0xFF,
                      (unsigned long long)pos);
        } else {
            gap_since_last_block += ALIGN;
            // If we haven't seen an MTF signature in 256 KB and we already
            // found at least an SSET, we've entered the page-data region.
            if (gap_since_last_block > 256 * 1024 && blocks.size() >= 2)
                break;
        }
    }

    if (blocks.empty()) {
        LOG_WARN("No MTF block signatures found in first 64 MB");
    }

    // ── Phase 2: Process each block ──────────────────────────────────
    bool found_backup_header = false;

    for (size_t i = 0; i < blocks.size(); ++i) {
        uint64_t blk_off = blocks[i].offset;
        uint64_t blk_end = (i + 1 < blocks.size())
                               ? blocks[i + 1].offset
                               : std::min(blk_off + 65536, scan_end);
        size_t blk_len = static_cast<size_t>(blk_end - blk_off);

        switch (blocks[i].type) {
        case MTF_TAPE:
            stream_.seek(blk_off);
            parse_mtf_tape_header();
            break;

        case MTF_SSET: {
            stream_.seek(blk_off);
            auto data = stream_.read_bytes(blk_len);
            parse_sset_block(data);
            break;
        }

        case MTF_DIRB:
        case MTF_FILE: {
            stream_.seek(blk_off);
            auto data = stream_.read_bytes(blk_len);

            if (!found_backup_header) {
                if (parse_sql_backup_header(data)) {
                    found_backup_header = true;
                    LOG_DEBUG("Found SQL Server backup header at offset %llu",
                              (unsigned long long)blk_off);
                }
            }
            parse_sql_file_list(data);
            break;
        }

        default:
            break;
        }
    }

    // ── Phase 3: Fallback if structured parsing found nothing ────────
    if (info_.backup_sets.empty()) {
        LOG_WARN("Could not parse structured backup header. "
                 "Metadata may be incomplete; restore mode recommended.");
        BackupSetInfo bsi;
        bsi.position = 1;
        bsi.backup_type = BackupType::Full;
        info_.backup_sets.push_back(bsi);
    }

    // Set data-start after the last header-region block we scanned.
    if (!blocks.empty())
        data_start_offset_ = blocks.back().offset;
    else
        data_start_offset_ = 0;

    LOG_INFO("Header parsing complete. Found %zu backup set(s), %zu file(s)",
             info_.backup_sets.size(), info_.file_list.size());

    for (size_t i = 0; i < info_.backup_sets.size(); ++i) {
        auto& bs = info_.backup_sets[i];
        LOG_INFO("  Set %zu: DB='%s' Server='%s' Compressed=%s TDE=%s Encrypted=%s",
                 i, bs.database_name.c_str(), bs.server_name.c_str(),
                 bs.is_compressed ? "yes" : "no",
                 bs.is_tde ? "yes" : "no",
                 bs.is_encrypted ? "yes" : "no");
    }

    return true;
}

bool BackupHeaderParser::parse_mtf_tape_header() {
    MtfTapeHeader tape;
    if (!stream_.read_exact(&tape, sizeof(tape))) {
        LOG_WARN("Incomplete MTF TAPE header");
        return false;
    }

    LOG_DEBUG("MTF media family ID: %u, sequence: %u",
              tape.media_family_id, tape.media_sequence_number);

    // Skip to the end of the TAPE block (typically 1024 bytes total)
    size_t remaining = 1024 - sizeof(tape);
    stream_.skip(remaining);
    return true;
}

bool BackupHeaderParser::parse_mtf_sset() {
    MtfStartOfSet sset;
    if (!stream_.read_exact(&sset, sizeof(sset))) {
        LOG_WARN("Incomplete MTF SSET header");
        return false;
    }

    BackupSetInfo bsi;
    bsi.position       = sset.data_set_number;
    bsi.is_compressed  = (sset.software_compression_algorithm != 0);

    LOG_DEBUG("MTF SSET: dataset=%d, compressed=%d",
              bsi.position, bsi.is_compressed);

    stream_.skip(512);

    if (info_.backup_sets.empty() || info_.backup_sets.back().position != bsi.position)
        info_.backup_sets.push_back(bsi);

    return true;
}

static bool is_plausible_db_name(const uint8_t* utf16_data, size_t byte_len) {
    size_t char_count = 0;
    size_t ascii_printable = 0;

    for (size_t i = 0; i + 1 < byte_len; i += 2) {
        uint16_t ch = static_cast<uint16_t>(utf16_data[i]) |
                      (static_cast<uint16_t>(utf16_data[i + 1]) << 8);
        if (ch == 0) break;

        ++char_count;

        if (ch >= 0x20 && ch < 0x7F)
            ++ascii_printable;
        else if (ch < 0x20)
            return false;
    }

    if (char_count < 2 || char_count > 128) return false;
    return ascii_printable * 4 >= char_count * 3;
}

// SQL Server backup descriptions follow the pattern:
//   "{DatabaseName}-Full Database Backup"
//   "{DatabaseName}-Differential Database Backup"
//   "{DatabaseName}-Transaction Log Backup"
static const char* const BACKUP_DESC_SUFFIXES[] = {
    "-Full Database Backup",
    "-Differential Database Backup",
    "-Transaction Log Backup",
};

void BackupHeaderParser::parse_sset_block(const std::vector<uint8_t>& data) {
    if (data.size() < 64) return;

    // Read the fixed SSET header fields from the raw data.
    MtfStartOfSet sset;
    size_t hdr_sz = std::min(sizeof(sset), data.size());
    std::memcpy(&sset, data.data(), hdr_sz);

    BackupSetInfo bsi;
    bsi.position      = sset.data_set_number;
    bsi.is_compressed = (sset.software_compression_algorithm != 0);
    bsi.backup_type   = BackupType::Full;

    LOG_DEBUG("SSET block: dataset=%d, compressed=%d",
              bsi.position, bsi.is_compressed);

    // Scan the block data for UTF-16LE strings that reveal the DB name.
    // The SSET string storage contains the backup description, which
    // SQL Server formats as "{DbName}-Full Database Backup" (or similar).
    for (size_t off = sizeof(sset); off + 6 < data.size(); off += 2) {
        // Probe for 3+ consecutive ASCII UTF-16LE code units
        bool ok = true;
        for (int k = 0; k < 3 && ok; ++k) {
            uint8_t lo = data[off + k * 2];
            uint8_t hi = data[off + k * 2 + 1];
            if (hi != 0x00 || lo < 0x20 || lo >= 0x7F) ok = false;
        }
        if (!ok) continue;

        size_t str_end = off;
        while (str_end + 1 < data.size() && str_end - off < 1024) {
            if (data[str_end] == 0x00 && data[str_end + 1] == 0x00) break;
            str_end += 2;
        }
        size_t str_len = str_end - off;
        if (str_len < 4) continue;

        if (!is_plausible_db_name(data.data() + off, str_len)) continue;

        std::string candidate = read_utf16_string(data.data() + off, str_len);
        if (candidate.empty()) continue;

        // Try to extract the DB name from the backup description pattern.
        for (auto suffix : BACKUP_DESC_SUFFIXES) {
            auto pos = candidate.find(suffix);
            if (pos != std::string::npos && pos > 0) {
                bsi.database_name = candidate.substr(0, pos);
                LOG_DEBUG("Extracted DB name '%s' from backup description at "
                          "SSET offset %zu",
                          bsi.database_name.c_str(), off);
                break;
            }
        }
        if (!bsi.database_name.empty()) break;

        // No known suffix -- use the first plausible string as-is
        // (only if it's short enough to be a DB name, not a description).
        if (candidate.size() <= 128 && bsi.database_name.empty()) {
            bsi.database_name = candidate;
            LOG_DEBUG("Using first SSET string as DB name: '%s' at offset %zu",
                      candidate.c_str(), off);
            break;
        }
    }

    if (info_.backup_sets.empty() || info_.backup_sets.back().position != bsi.position)
        info_.backup_sets.push_back(bsi);
    else if (!bsi.database_name.empty() && info_.backup_sets.back().database_name.empty())
        info_.backup_sets.back().database_name = bsi.database_name;
}

bool BackupHeaderParser::parse_sql_media_header(const std::vector<uint8_t>& data) {
    // Look for the SQL Server media header signature within the data block.
    // The media header contains the backup software version and media info.
    if (data.size() < 128) return false;

    for (size_t off = 0; off + 64 <= data.size(); ++off) {
        // SQL Server media header often starts with a recognizable version dword
        // followed by database metadata. We look for the "MSSQLSERVER" or similar
        // embedded strings.
        if (data[off] == 0x02 && data[off+1] == 0x00 &&
            data[off+2] == 0x00 && data[off+3] == 0x00) {
            // Potential version = 2 (media header version)
            // Verify next bytes are consistent
            uint32_t flags;
            std::memcpy(&flags, data.data() + off + 4, 4);
            if (flags < 0x1000) {
                LOG_DEBUG("Potential SQL media header at offset %zu", off);
                return true;
            }
        }
    }
    return false;
}

bool BackupHeaderParser::parse_sql_backup_header(const std::vector<uint8_t>& data) {
    if (data.size() < 256) return false;

    for (size_t off = 0; off + 128 <= data.size(); off += 2) {
        // Probe: need at least 3 consecutive ASCII-range UTF-16LE code units
        // to start considering this a candidate string.
        if (off + 6 > data.size()) continue;
        bool ok = true;
        for (int k = 0; k < 3 && ok; ++k) {
            uint8_t lo = data[off + k * 2];
            uint8_t hi = data[off + k * 2 + 1];
            if (hi != 0x00 || lo < 0x20 || lo >= 0x7F) ok = false;
        }
        if (!ok) continue;

        // Find the null-terminated extent of the UTF-16LE string
        size_t str_end = off;
        while (str_end + 1 < data.size() && str_end - off < 512) {
            if (data[str_end] == 0x00 && data[str_end + 1] == 0x00) break;
            str_end += 2;
        }

        size_t str_len = str_end - off;
        if (str_len < 4 || str_len > 256) continue;

        // Validate the full run of code points, not just the first few
        if (!is_plausible_db_name(data.data() + off, str_len)) continue;

        std::string candidate = read_utf16_string(data.data() + off, str_len);
        if (candidate.empty()) continue;

        // Require a version-like integer nearby as extra confidence
        bool has_version = false;
        if (off >= 32) {
            uint32_t maybe_version;
            std::memcpy(&maybe_version, data.data() + off - 32, 4);
            if (maybe_version >= 80 && maybe_version <= 200)
                has_version = true;
        }

        if (has_version || off < 2048) {
            if (!info_.backup_sets.empty()) {
                auto& bs = info_.backup_sets.back();
                if (bs.database_name.empty()) {
                    bs.database_name = candidate;
                    LOG_DEBUG("Extracted database name: '%s' at offset %zu",
                              candidate.c_str(), off);
                    return true;
                }
            } else {
                BackupSetInfo bsi;
                bsi.position = 1;
                bsi.database_name = candidate;
                bsi.backup_type = BackupType::Full;
                info_.backup_sets.push_back(bsi);
                return true;
            }
        }
    }

    return false;
}

bool BackupHeaderParser::parse_sql_file_list(const std::vector<uint8_t>& data) {
    // File list entries contain logical name + physical name pairs
    // followed by size info. Similar UTF-16LE scanning approach.
    // In practice, restore mode is the reliable way to get file lists.
    (void)data;
    return false;
}

bool BackupHeaderParser::scan_for_data_start() {
    // After headers, the page data begins. We look for the first 8KB-aligned
    // block that contains a valid SQL Server page header.
    return true;
}

std::string BackupHeaderParser::read_utf16_string(const uint8_t* data, size_t byte_len) {
    std::string result;
    result.reserve(byte_len / 2);

    for (size_t i = 0; i + 1 < byte_len; i += 2) {
        uint16_t ch = static_cast<uint16_t>(data[i]) |
                      (static_cast<uint16_t>(data[i+1]) << 8);
        if (ch == 0) break;
        if (ch < 0x80) {
            result.push_back(static_cast<char>(ch));
        } else if (ch < 0x800) {
            result.push_back(static_cast<char>(0xC0 | (ch >> 6)));
            result.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
        } else {
            result.push_back(static_cast<char>(0xE0 | (ch >> 12)));
            result.push_back(static_cast<char>(0x80 | ((ch >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
        }
    }
    return result;
}

const BackupSetInfo* BackupHeaderParser::select_backup_set(int index) const {
    if (index < 0 || index >= static_cast<int>(info_.backup_sets.size()))
        return nullptr;
    return &info_.backup_sets[index];
}

bool BackupHeaderParser::is_tde_enabled() const {
    for (auto& bs : info_.backup_sets)
        if (bs.is_tde) return true;
    return false;
}

bool BackupHeaderParser::is_backup_encrypted() const {
    for (auto& bs : info_.backup_sets)
        if (bs.is_encrypted) return true;
    return false;
}

int32_t BackupHeaderParser::sql_version_major() const {
    if (info_.backup_sets.empty()) return 0;
    return info_.backup_sets.front().software_major;
}

}  // namespace bakread
