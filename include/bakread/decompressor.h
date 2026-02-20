#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// Decompressor for SQL Server compressed backup blocks
//
// SQL Server uses a modified compression scheme for backup compression:
//   - Each compressed block has a small header indicating compressed/raw size
//   - The compression algorithm is a variant of LZ77 (not standard gzip/deflate)
//   - For some versions, standard deflate may also be used
//
// This module provides a unified interface that:
//   1. Detects whether a block is compressed
//   2. Decompresses using the appropriate algorithm
//   3. Falls back to raw passthrough if not compressed
// -------------------------------------------------------------------------

// Compressed block header as written by SQL Server
#pragma pack(push, 1)
struct CompressedBlockHeader {
    uint16_t magic;             // 0xDAC0 for compressed, other for raw
    uint16_t header_size;       // Size of this header
    uint32_t compressed_size;   // Size of compressed data
    uint32_t uncompressed_size; // Size of original data
};
#pragma pack(pop)

class Decompressor {
public:
    Decompressor();
    ~Decompressor();

    Decompressor(const Decompressor&) = delete;
    Decompressor& operator=(const Decompressor&) = delete;

    // Check if a data block appears to be compressed
    static bool is_compressed(const uint8_t* data, size_t len);

    // Decompress a block. Returns decompressed data.
    // If the block is not compressed, returns a copy of the input.
    std::vector<uint8_t> decompress(const uint8_t* data, size_t len);

    // Decompress into a pre-allocated buffer. Returns actual decompressed size.
    // Returns 0 on failure.
    size_t decompress_into(const uint8_t* src, size_t src_len,
                           uint8_t* dst, size_t dst_capacity);

    // Get expected decompressed size from the block header (if compressed)
    static size_t expected_decompressed_size(const uint8_t* data, size_t len);

private:
    // SQL Server's custom LZ77 decompression
    size_t decompress_sqlserver_lz(const uint8_t* src, size_t src_len,
                                   uint8_t* dst, size_t dst_capacity);

    // Standard deflate fallback
    size_t decompress_deflate(const uint8_t* src, size_t src_len,
                              uint8_t* dst, size_t dst_capacity);

    // Working buffer for intermediate operations
    std::vector<uint8_t> work_buffer_;
};

}  // namespace bakread
