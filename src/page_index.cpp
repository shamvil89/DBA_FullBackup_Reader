#include "bakread/page_index.h"
#include "bakread/logging.h"

#include <fstream>
#include <cstring>

namespace bakread {

void PageIndex::add_entry(int32_t file_id, int32_t page_id, const PageIndexEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t key = make_page_key(file_id, page_id);
    entries_[key] = entry;
}

bool PageIndex::lookup(int32_t file_id, int32_t page_id, PageIndexEntry& entry) const {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t key = make_page_key(file_id, page_id);
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        entry = it->second;
        return true;
    }
    return false;
}

bool PageIndex::contains(int32_t file_id, int32_t page_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t key = make_page_key(file_id, page_id);
    return entries_.find(key) != entries_.end();
}

std::vector<int64_t> PageIndex::get_pages_by_type(IndexedPageType type) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int64_t> result;
    for (const auto& [key, entry] : entries_) {
        if (entry.page_type == static_cast<uint8_t>(type)) {
            result.push_back(key);
        }
    }
    return result;
}

std::vector<int64_t> PageIndex::get_pages_by_object(uint32_t object_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int64_t> result;
    for (const auto& [key, entry] : entries_) {
        if (entry.object_id == object_id) {
            result.push_back(key);
        }
    }
    return result;
}

size_t PageIndex::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

size_t PageIndex::memory_usage_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    // Estimate: key (8) + entry (16) + hash overhead (~16)
    return entries_.size() * 40 + sizeof(PageIndex);
}

void PageIndex::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
}

std::vector<int64_t> PageIndex::get_system_pages() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int64_t> result;
    for (const auto& [key, entry] : entries_) {
        auto type = static_cast<IndexedPageType>(entry.page_type);
        if (type == IndexedPageType::System ||
            type == IndexedPageType::Boot ||
            type == IndexedPageType::FileHeader) {
            result.push_back(key);
        }
    }
    return result;
}

bool PageIndex::save_to_file(const std::string& path) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ofstream file(path, std::ios::binary);
    if (!file.is_open()) {
        LOG_ERROR("Failed to create index file: %s", path.c_str());
        return false;
    }

    // Build header
    IndexFileHeader header{};
    std::memcpy(header.magic, "BAKRIDX", 8);
    header.version = 1;
    header.entry_count = static_cast<uint32_t>(entries_.size());
    header.total_pages = entries_.size();

    // Count page types
    for (const auto& [key, entry] : entries_) {
        if (entry.page_type == static_cast<uint8_t>(IndexedPageType::Data)) {
            header.data_pages++;
        } else if (entry.page_type == static_cast<uint8_t>(IndexedPageType::System)) {
            header.system_pages++;
        }
    }

    // Write header
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));

    // Write entries as (key, entry) pairs
    for (const auto& [key, entry] : entries_) {
        file.write(reinterpret_cast<const char*>(&key), sizeof(key));
        file.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
    }

    LOG_INFO("Saved page index: %zu entries to %s", entries_.size(), path.c_str());
    return true;
}

bool PageIndex::load_from_file(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        LOG_DEBUG("Index file not found: %s", path.c_str());
        return false;
    }

    // Read header
    IndexFileHeader header{};
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (std::memcmp(header.magic, "BAKRIDX", 7) != 0) {
        LOG_ERROR("Invalid index file magic: %s", path.c_str());
        return false;
    }

    if (header.version != 1) {
        LOG_ERROR("Unsupported index version %u: %s", header.version, path.c_str());
        return false;
    }

    // Read entries
    entries_.clear();
    entries_.reserve(header.entry_count);

    for (uint32_t i = 0; i < header.entry_count; ++i) {
        int64_t key;
        PageIndexEntry entry;
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        file.read(reinterpret_cast<char*>(&entry), sizeof(entry));
        entries_[key] = entry;
    }

    LOG_INFO("Loaded page index: %zu entries from %s", entries_.size(), path.c_str());
    return true;
}

}  // namespace bakread
