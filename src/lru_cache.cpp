#include "bakread/lru_cache.h"

#include <algorithm>
#include <cstring>

namespace bakread {

LRUPageCache::LRUPageCache(size_t max_pages)
    : max_pages_(max_pages)
{
}

bool LRUPageCache::get(int64_t key, uint8_t* out_page) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = lookup_.find(key);
    if (it == lookup_.end()) {
        ++misses_;
        return false;
    }

    // Move to front (most recently used)
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    std::memcpy(out_page, it->second->data.data(), PAGE_SIZE);
    ++hits_;
    return true;
}

void LRUPageCache::put(int64_t key, const uint8_t* page_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = lookup_.find(key);
    if (it != lookup_.end()) {
        // Update existing entry and move to front
        std::memcpy(it->second->data.data(), page_data, PAGE_SIZE);
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return;
    }

    // Evict if at capacity
    evict_if_needed();

    // Add new entry at front
    CacheEntry entry;
    entry.key = key;
    entry.data.resize(PAGE_SIZE);
    std::memcpy(entry.data.data(), page_data, PAGE_SIZE);

    lru_list_.push_front(std::move(entry));
    lookup_[key] = lru_list_.begin();
}

bool LRUPageCache::contains(int64_t key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lookup_.find(key) != lookup_.end();
}

void LRUPageCache::remove(int64_t key) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = lookup_.find(key);
    if (it != lookup_.end()) {
        lru_list_.erase(it->second);
        lookup_.erase(it);
    }
}

void LRUPageCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_list_.clear();
    lookup_.clear();
    hits_ = 0;
    misses_ = 0;
}

size_t LRUPageCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lookup_.size();
}

size_t LRUPageCache::memory_usage_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    // Each entry: 8KB data + key (8) + list node overhead (~48) + map entry (~40)
    return lookup_.size() * (PAGE_SIZE + 96) + sizeof(LRUPageCache);
}

double LRUPageCache::hit_rate() const {
    uint64_t total = hits_ + misses_;
    if (total == 0) return 0.0;
    return static_cast<double>(hits_) / static_cast<double>(total);
}

void LRUPageCache::resize(size_t max_pages) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_pages_ = max_pages;
    
    // Evict excess entries
    while (lookup_.size() > max_pages_) {
        auto& back = lru_list_.back();
        lookup_.erase(back.key);
        lru_list_.pop_back();
    }
}

void LRUPageCache::evict_if_needed() {
    // Called with lock held
    while (lookup_.size() >= max_pages_ && !lru_list_.empty()) {
        auto& back = lru_list_.back();
        lookup_.erase(back.key);
        lru_list_.pop_back();
    }
}

}  // namespace bakread
