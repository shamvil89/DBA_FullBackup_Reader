#pragma once

#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace bakread {

// Thread-safe LRU cache for 8KB pages
// Key: int64_t (file_id << 32 | page_id)
// Value: 8KB page buffer
class LRUPageCache {
public:
    static constexpr size_t PAGE_SIZE = 8192;

    explicit LRUPageCache(size_t max_pages = 1024);
    ~LRUPageCache() = default;

    LRUPageCache(const LRUPageCache&) = delete;
    LRUPageCache& operator=(const LRUPageCache&) = delete;

    // Try to get a page from cache. Returns true if found.
    bool get(int64_t key, uint8_t* out_page);

    // Add/update a page in cache. Evicts LRU if full.
    void put(int64_t key, const uint8_t* page_data);

    // Check if page is cached
    bool contains(int64_t key) const;

    // Remove a specific page
    void remove(int64_t key);

    // Clear all cached pages
    void clear();

    // Statistics
    size_t size() const;
    size_t capacity() const { return max_pages_; }
    size_t memory_usage_bytes() const;
    uint64_t hits() const { return hits_; }
    uint64_t misses() const { return misses_; }
    double hit_rate() const;

    // Resize cache (clears existing entries)
    void resize(size_t max_pages);

private:
    struct CacheEntry {
        int64_t key;
        std::vector<uint8_t> data;  // 8KB page
    };

    using ListIterator = std::list<CacheEntry>::iterator;

    mutable std::mutex mutex_;
    size_t max_pages_;
    std::list<CacheEntry> lru_list_;  // Front = most recent, back = least recent
    std::unordered_map<int64_t, ListIterator> lookup_;

    // Statistics
    mutable uint64_t hits_ = 0;
    mutable uint64_t misses_ = 0;

    void evict_if_needed();
};

}  // namespace bakread
