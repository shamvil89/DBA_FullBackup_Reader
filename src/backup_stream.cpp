#include "bakread/backup_stream.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cstring>
#include <filesystem>

namespace bakread {

BackupStream::BackupStream(const std::string& path, size_t buffer_size)
    : buffer_(buffer_size)
{
    namespace fs = std::filesystem;

    if (!fs::exists(path))
        throw FileIOError("File not found: " + path);

    file_size_ = fs::file_size(path);
    if (file_size_ == 0)
        throw FileIOError("File is empty: " + path);

    file_.open(path, std::ios::binary);
    if (!file_.is_open())
        throw FileIOError("Cannot open file: " + path);

    LOG_INFO("Opened backup file: %s (%.2f GB)",
             path.c_str(), file_size_ / (1024.0 * 1024.0 * 1024.0));
}

BackupStream::~BackupStream() = default;

uint64_t BackupStream::position()  const { return logical_pos_; }
uint64_t BackupStream::file_size() const { return file_size_; }
bool     BackupStream::eof()       const { return logical_pos_ >= file_size_; }

double BackupStream::progress_pct() const {
    if (file_size_ == 0) return 100.0;
    return static_cast<double>(logical_pos_) / static_cast<double>(file_size_) * 100.0;
}

void BackupStream::refill() {
    if (file_.eof() || !file_.good()) {
        buf_pos_ = buf_len_ = 0;
        return;
    }
    file_.read(reinterpret_cast<char*>(buffer_.data()),
               static_cast<std::streamsize>(buffer_.size()));
    buf_len_ = static_cast<size_t>(file_.gcount());
    buf_pos_ = 0;
}

size_t BackupStream::read(void* dest, size_t count) {
    auto out = static_cast<uint8_t*>(dest);
    size_t total = 0;

    while (total < count) {
        if (buf_pos_ >= buf_len_) {
            refill();
            if (buf_len_ == 0) break;
        }
        size_t avail = buf_len_ - buf_pos_;
        size_t chunk = std::min(avail, count - total);
        std::memcpy(out + total, buffer_.data() + buf_pos_, chunk);
        buf_pos_     += chunk;
        logical_pos_ += chunk;
        total        += chunk;
    }
    return total;
}

bool BackupStream::read_exact(void* dest, size_t count) {
    return read(dest, count) == count;
}

bool BackupStream::skip(uint64_t count) {
    // Use buffer where possible, then seek
    while (count > 0) {
        if (buf_pos_ < buf_len_) {
            size_t avail = buf_len_ - buf_pos_;
            size_t chunk = static_cast<size_t>(std::min<uint64_t>(avail, count));
            buf_pos_     += chunk;
            logical_pos_ += chunk;
            count        -= chunk;
        } else {
            // Seek forward in file
            file_.seekg(static_cast<std::streamoff>(count), std::ios::cur);
            logical_pos_ += count;
            buf_pos_ = buf_len_ = 0;
            count = 0;
        }
    }
    return !file_.fail();
}

bool BackupStream::seek(uint64_t offset) {
    file_.clear();
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    logical_pos_ = offset;
    buf_pos_ = buf_len_ = 0;
    return !file_.fail();
}

bool BackupStream::peek(void* dest, size_t count) {
    uint64_t saved = logical_pos_;
    size_t saved_buf_pos = buf_pos_;
    size_t saved_buf_len = buf_len_;

    bool ok = read_exact(dest, count);

    // Restore position
    logical_pos_ = saved;
    buf_pos_ = saved_buf_pos;

    if (!ok) {
        // If we couldn't peek from buffer, need to re-seek
        file_.clear();
        file_.seekg(static_cast<std::streamoff>(saved), std::ios::beg);
        buf_pos_ = buf_len_ = 0;
    }
    return ok;
}

bool BackupStream::read_block_header(MtfBlockHeader& hdr) {
    return read_exact(&hdr, sizeof(hdr));
}

std::vector<uint8_t> BackupStream::read_bytes(size_t count) {
    std::vector<uint8_t> data(count);
    size_t got = read(data.data(), count);
    data.resize(got);
    return data;
}

}  // namespace bakread
