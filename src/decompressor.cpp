#include "bakread/decompressor.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cstring>

#ifdef BAKREAD_HAS_ZLIB
#include <zlib.h>
#endif

namespace bakread {

static constexpr uint16_t SQL_COMPRESS_MAGIC = 0xDAC0;

Decompressor::Decompressor()
    : work_buffer_(256 * 1024)
{
}

Decompressor::~Decompressor() = default;

bool Decompressor::is_compressed(const uint8_t* data, size_t len) {
    if (len < sizeof(CompressedBlockHeader)) return false;

    uint16_t magic;
    std::memcpy(&magic, data, 2);
    return magic == SQL_COMPRESS_MAGIC;
}

size_t Decompressor::expected_decompressed_size(const uint8_t* data, size_t len) {
    if (len < sizeof(CompressedBlockHeader)) return len;

    CompressedBlockHeader hdr;
    std::memcpy(&hdr, data, sizeof(hdr));

    if (hdr.magic != SQL_COMPRESS_MAGIC) return len;
    return hdr.uncompressed_size;
}

std::vector<uint8_t> Decompressor::decompress(const uint8_t* data, size_t len) {
    if (!is_compressed(data, len)) {
        return std::vector<uint8_t>(data, data + len);
    }

    CompressedBlockHeader hdr;
    std::memcpy(&hdr, data, sizeof(hdr));

    std::vector<uint8_t> result(hdr.uncompressed_size);
    size_t actual = decompress_into(data, len, result.data(), result.size());

    if (actual == 0) {
        throw CompressionError("Failed to decompress block");
    }

    result.resize(actual);
    return result;
}

size_t Decompressor::decompress_into(const uint8_t* src, size_t src_len,
                                      uint8_t* dst, size_t dst_capacity) {
    if (!is_compressed(src, src_len)) {
        size_t copy_len = std::min(src_len, dst_capacity);
        std::memcpy(dst, src, copy_len);
        return copy_len;
    }

    CompressedBlockHeader hdr;
    std::memcpy(&hdr, src, sizeof(hdr));

    const uint8_t* payload = src + hdr.header_size;
    size_t payload_len = src_len - hdr.header_size;

    if (payload_len < hdr.compressed_size) {
        LOG_WARN("Compressed block payload truncated: expected %u, got %zu",
                 hdr.compressed_size, payload_len);
        payload_len = std::min(payload_len, static_cast<size_t>(hdr.compressed_size));
    }

    // Try SQL Server's LZ compression first
    size_t result = decompress_sqlserver_lz(payload, payload_len, dst, dst_capacity);
    if (result > 0) return result;

    // Fallback to standard deflate
    result = decompress_deflate(payload, payload_len, dst, dst_capacity);
    if (result > 0) return result;

    LOG_ERROR("All decompression methods failed for block (compressed=%u, "
              "uncompressed=%u)", hdr.compressed_size, hdr.uncompressed_size);
    return 0;
}

size_t Decompressor::decompress_sqlserver_lz(const uint8_t* src, size_t src_len,
                                              uint8_t* dst, size_t dst_capacity) {
    // SQL Server backup compression uses a LZ77-variant (LZXPRESS-like) scheme:
    //   - A flags byte where each bit indicates literal (0) or match (1)
    //   - Literals are copied directly
    //   - Matches encode (offset, length) pairs
    //
    // This implementation handles the common LZXPRESS plain format used in
    // SQL Server backups. The compressed stream consists of:
    //   - Groups of 32 items controlled by a 32-bit flags dword
    //   - Flag bit = 0: next byte is a literal
    //   - Flag bit = 1: next 2 bytes encode a (offset, length) match

    if (src_len < 4) return 0;

    size_t si = 0;  // source index
    size_t di = 0;  // destination index

    while (si < src_len && di < dst_capacity) {
        if (si + 4 > src_len) break;

        uint32_t flags;
        std::memcpy(&flags, src + si, 4);
        si += 4;

        for (int bit = 0; bit < 32 && si < src_len && di < dst_capacity; ++bit) {
            if (!(flags & (1u << bit))) {
                // Literal byte
                dst[di++] = src[si++];
            } else {
                // Match reference
                if (si + 2 > src_len) return di;

                uint16_t match_info;
                std::memcpy(&match_info, src + si, 2);
                si += 2;

                uint32_t match_offset = (match_info >> 3) + 1;
                uint32_t match_length = (match_info & 0x07) + 3;

                // Extended length encoding
                if ((match_info & 0x07) == 0x07) {
                    if (si >= src_len) return di;
                    uint8_t extra = src[si++];
                    match_length = extra + 10;

                    if (extra == 0xFF) {
                        if (si + 2 > src_len) return di;
                        uint16_t ext16;
                        std::memcpy(&ext16, src + si, 2);
                        si += 2;
                        match_length = ext16;
                        if (match_length == 0) {
                            if (si + 4 > src_len) return di;
                            uint32_t ext32;
                            std::memcpy(&ext32, src + si, 4);
                            si += 4;
                            match_length = ext32;
                        }
                    }
                }

                if (match_offset > di) {
                    LOG_DEBUG("LZ match offset %u exceeds output position %zu",
                              match_offset, di);
                    return 0;
                }

                // Copy match (byte-by-byte to handle overlapping copies)
                for (uint32_t j = 0; j < match_length && di < dst_capacity; ++j) {
                    dst[di] = dst[di - match_offset];
                    ++di;
                }
            }
        }
    }

    return di;
}

size_t Decompressor::decompress_deflate(const uint8_t* src, size_t src_len,
                                         uint8_t* dst, size_t dst_capacity) {
#ifdef BAKREAD_HAS_ZLIB
    z_stream strm = {};
    strm.next_in  = const_cast<Bytef*>(src);
    strm.avail_in = static_cast<uInt>(src_len);
    strm.next_out = dst;
    strm.avail_out = static_cast<uInt>(dst_capacity);

    // Try raw deflate first (no zlib/gzip header)
    int ret = inflateInit2(&strm, -MAX_WBITS);
    if (ret != Z_OK) return 0;

    ret = inflate(&strm, Z_FINISH);
    size_t result = strm.total_out;
    inflateEnd(&strm);

    if (ret == Z_STREAM_END) return result;

    // Retry with zlib header
    strm = {};
    strm.next_in   = const_cast<Bytef*>(src);
    strm.avail_in  = static_cast<uInt>(src_len);
    strm.next_out  = dst;
    strm.avail_out = static_cast<uInt>(dst_capacity);

    ret = inflateInit(&strm);
    if (ret != Z_OK) return 0;

    ret = inflate(&strm, Z_FINISH);
    result = strm.total_out;
    inflateEnd(&strm);

    if (ret == Z_STREAM_END) return result;
#else
    (void)src; (void)src_len; (void)dst; (void)dst_capacity;
#endif
    return 0;
}

}  // namespace bakread
