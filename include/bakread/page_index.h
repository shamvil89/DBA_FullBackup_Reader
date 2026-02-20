#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace bakread {

// Page types for filtering during extraction
enum class IndexedPageType : uint8_t {
    Unknown     = 0,
    Data        = 1,   // User data pages
    Index       = 2,   // Index pages
    TextMix     = 3,   // LOB data
    TextTree    = 4,   // LOB tree
    System      = 5,   // System catalog pages
    IAM         = 10,  // Index Allocation Map
    GAM         = 8,   // Global Allocation Map
    SGAM        = 9,   // Shared GAM
    PFS         = 11,  // Page Free Space
    Boot        = 13,  // Boot page
    FileHeader  = 15,  // File header
};

// Entry in the page index - 24 bytes per page
struct PageIndexEntry {
    uint8_t  stripe_index;    // Which stripe file (0-255)
    uint8_t  page_type;       // IndexedPageType
    uint8_t  reserved[2];     // Alignment padding
    uint32_t object_id;       // Page header m_objId (for filtering)
    uint64_t file_offset;     // Byte offset within the stripe file
};

// Compact key for page lookup
inline int64_t make_page_key(int32_t file_id, int32_t page_id) {
    return (static_cast<int64_t>(file_id) << 32) | static_cast<uint32_t>(page_id);
}

inline void split_page_key(int64_t key, int32_t& file_id, int32_t& page_id) {
    file_id = static_cast<int32_t>(key >> 32);
    page_id = static_cast<int32_t>(key & 0xFFFFFFFF);
}

// In-memory page index
// Maps (file_id, page_id) -> location in stripe file
class PageIndex {
public:
    PageIndex() = default;
    ~PageIndex() = default;

    // Add a page entry (thread-safe)
    void add_entry(int32_t file_id, int32_t page_id, const PageIndexEntry& entry);

    // Lookup a page entry (thread-safe)
    bool lookup(int32_t file_id, int32_t page_id, PageIndexEntry& entry) const;

    // Check if page exists
    bool contains(int32_t file_id, int32_t page_id) const;

    // Get all pages of a specific type
    std::vector<int64_t> get_pages_by_type(IndexedPageType type) const;

    // Get all pages for a specific object_id
    std::vector<int64_t> get_pages_by_object(uint32_t object_id) const;

    // Statistics
    size_t size() const;
    size_t memory_usage_bytes() const;

    // Serialization to/from index file
    bool save_to_file(const std::string& path) const;
    bool load_from_file(const std::string& path);

    // Clear all entries
    void clear();

    // Get all system/catalog page keys (for metadata extraction)
    std::vector<int64_t> get_system_pages() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<int64_t, PageIndexEntry> entries_;
};

// Index file header (for persistence)
struct IndexFileHeader {
    char     magic[8];        // "BAKRIDX\0"
    uint32_t version;         // Format version
    uint32_t entry_count;     // Number of entries
    uint64_t total_pages;     // Total pages scanned
    uint64_t data_pages;      // Data pages found
    uint64_t system_pages;    // System pages found
    char     reserved[24];    // Future use (adjusted for 64-byte alignment)
};

static_assert(sizeof(IndexFileHeader) == 64, "IndexFileHeader must be 64 bytes");
static_assert(sizeof(PageIndexEntry) == 16, "PageIndexEntry should be 16 bytes");

}  // namespace bakread
