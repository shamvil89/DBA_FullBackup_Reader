#pragma once

#include "bakread/types.h"

#include <cstdint>
#include <cstring>

namespace bakread {

// -------------------------------------------------------------------------
// SQL Server page constants
// -------------------------------------------------------------------------
static constexpr size_t PAGE_SIZE       = 8192;
static constexpr size_t PAGE_HEADER_SIZE = 96;
static constexpr size_t PAGE_DATA_SIZE  = PAGE_SIZE - PAGE_HEADER_SIZE;

// -------------------------------------------------------------------------
// Page types (m_type field)
// -------------------------------------------------------------------------
enum class PageType : uint8_t {
    Data          = 1,
    Index         = 2,
    TextMix       = 3,
    TextTree      = 4,
    Sort          = 7,
    GAM           = 8,
    SGAM          = 9,
    IAM           = 10,
    PFS           = 11,
    Boot          = 13,
    FileHeader    = 15,
    DiffMap       = 16,
    MLMap         = 17,
    // System catalog pages use Data type = 1 with specific object IDs
};

// -------------------------------------------------------------------------
// SQL Server page header (96 bytes)
//
// On-disk layout for SQL Server 2005-2022 pages.
// Offsets verified against real backup file analysis.
//
//   0x00: header_version (1)    0x01: type (1)
//   0x02: type_flag_bits (1)    0x03: level (1)
//   0x04: flag_bits (2)         0x06: index_id (2)
//   0x08: prev_page (4)         0x0C: prev_file (2)
//   0x0E: pminlen (2)
//   0x10: next_page (4)         0x14: next_file (2)
//   0x16: slot_count (2)
//   0x18: obj_id (4)
//   0x1C: free_count (2)        0x1E: free_data (2)
//   0x20: this_page (4)         0x24: this_file (2)
//   0x26: reserved_count (2)
//   0x28: lsn (4+4+2)           0x32: xact_reserved (2)
//   0x34: xdes_id (4+4)         0x3C: ghost_rec_count (2)
//   0x3E: torn_bits (2)         0x40: _reserved[32]
// -------------------------------------------------------------------------
#pragma pack(push, 1)
struct PageHeader {
    uint8_t  header_version;     // 0x00  0x01 for current format
    uint8_t  type;               // 0x01  PageType
    uint8_t  type_flag_bits;     // 0x02
    uint8_t  level;              // 0x03  B-tree level (0 = leaf)
    uint16_t flag_bits;          // 0x04
    uint16_t index_id;           // 0x06  hobt / allocation unit metadata

    uint32_t prev_page;          // 0x08  Previous page ID
    uint16_t prev_file;          // 0x0C  Previous page file ID
    uint16_t pminlen;            // 0x0E  Minimum record length

    uint32_t next_page;          // 0x10  Next page ID
    uint16_t next_file;          // 0x14  Next page file ID
    uint16_t slot_count;         // 0x16  Number of records on this page

    uint32_t obj_id;             // 0x18  Object / allocation unit ID
    uint16_t free_count;         // 0x1C  Free bytes on this page
    uint16_t free_data;          // 0x1E  Offset to first free byte

    uint32_t this_page;          // 0x20  This page's page ID
    uint16_t this_file;          // 0x24  This page's file ID
    uint16_t reserved_count;     // 0x26

    // LSN (Log Sequence Number)
    uint32_t lsn_file;           // 0x28
    uint32_t lsn_offset;         // 0x2C
    uint16_t lsn_slot;           // 0x30

    uint16_t xact_reserved;      // 0x32
    uint32_t xdes_id1;           // 0x34
    uint32_t xdes_id2;           // 0x38

    uint16_t ghost_rec_count;    // 0x3C
    uint16_t torn_bits;          // 0x3E  or checksum depending on config

    uint8_t  _reserved[32];      // 0x40  padding to 96 bytes
};
static_assert(sizeof(PageHeader) == PAGE_HEADER_SIZE, "PageHeader must be 96 bytes");
#pragma pack(pop)

// -------------------------------------------------------------------------
// Page accessor helpers
// -------------------------------------------------------------------------

inline PageType get_page_type(const PageHeader& hdr) {
    return static_cast<PageType>(hdr.type);
}

inline PageId get_page_id(const PageHeader& hdr) {
    return { static_cast<int32_t>(hdr.this_file),
             static_cast<int32_t>(hdr.this_page) };
}

inline PageId get_prev_page(const PageHeader& hdr) {
    return { static_cast<int32_t>(hdr.prev_file),
             static_cast<int32_t>(hdr.prev_page) };
}

inline PageId get_next_page(const PageHeader& hdr) {
    return { static_cast<int32_t>(hdr.next_file),
             static_cast<int32_t>(hdr.next_page) };
}

// Get the slot array entry (2-byte record offset) at index i.
// The slot array grows backward from the end of the page.
inline uint16_t get_slot_offset(const uint8_t* page_data, int slot_index) {
    size_t array_pos = PAGE_SIZE - 2 * (slot_index + 1);
    uint16_t offset;
    std::memcpy(&offset, page_data + array_pos, 2);
    return offset;
}

// Get pointer to the data area of a page (past the 96-byte header)
inline const uint8_t* page_data_area(const uint8_t* page) {
    return page + PAGE_HEADER_SIZE;
}

// -------------------------------------------------------------------------
// Record (row) status bits -- first byte of each record on a data page
// -------------------------------------------------------------------------
namespace RecordStatus {
    static constexpr uint8_t HasNullBitmap  = 0x10;
    static constexpr uint8_t HasVarColumns  = 0x20;
    static constexpr uint8_t HasVersionTag  = 0x40;
    static constexpr uint8_t ForwardedStub  = 0x04;
    static constexpr uint8_t GhostForward   = 0x02;
    static constexpr uint8_t TypeMask       = 0x07;
    static constexpr uint8_t PrimaryRecord  = 0x00;
    static constexpr uint8_t Forwarded      = 0x01;
    static constexpr uint8_t ForwardingStub = 0x02;
    static constexpr uint8_t IndexRecord    = 0x06;
}

// -------------------------------------------------------------------------
// IAM page structure (for tracking allocations)
// -------------------------------------------------------------------------
struct IamPageHeader {
    PageHeader base;
    // After the standard header, the IAM page contains:
    //   Bytes 96-99: Sequence number
    //   Bytes 100-103: status
    //   Bytes 104-109: start page (6 bytes: 4-byte page + 2-byte file)
    //   ... allocation bitmap follows
};

// Get the start page referenced by an IAM page
inline PageId iam_start_page(const uint8_t* page) {
    uint32_t pg;
    uint16_t fi;
    std::memcpy(&pg, page + 104, 4);
    std::memcpy(&fi, page + 108, 2);
    return { static_cast<int32_t>(fi), static_cast<int32_t>(pg) };
}

// Check if a specific extent (relative to start_page) is allocated in the IAM bitmap
inline bool iam_extent_allocated(const uint8_t* page, int extent_index) {
    // IAM bitmap starts at offset 194 (after single-page allocations at 110-193)
    int byte_offset = 194 + (extent_index / 8);
    int bit_offset  = extent_index % 8;
    if (byte_offset >= static_cast<int>(PAGE_SIZE)) return false;
    return (page[byte_offset] & (1 << bit_offset)) != 0;
}

}  // namespace bakread
