#pragma once

#include "bakread/page.h"
#include "bakread/types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// RowDecoder -- parses SQL Server FixedVar row format from data pages
//
// SQL Server stores rows in the FixedVar format (most common):
//
//  [Status A] [Status B] [Fixed-length data offset] [Fixed data...]
//  [Null bitmap] [Var col count] [Var col end offsets...] [Var data...]
//
// Byte layout:
//   0      : Status byte A (record type, flags)
//   1      : Status byte B (additional flags)
//   2-3    : Fixed-length data end offset (2 bytes, little-endian)
//   4..N   : Fixed-length column data
//   N..N+C : Null bitmap (ceil(num_columns / 8) bytes)
//   (if HasVarColumns):
//     2 bytes : Number of variable-length columns
//     2*V bytes : End offset for each variable-length column
//     Variable-length column data follows
// -------------------------------------------------------------------------
class RowDecoder {
public:
    explicit RowDecoder(const TableSchema& schema);

    // Decode a single row from a page at the given record offset.
    // page_data: pointer to the full 8KB page
    // record_offset: offset within the page where the record starts
    // Returns true on success, populates out_row.
    bool decode_row(const uint8_t* page_data, uint16_t record_offset,
                    Row& out_row) const;

    // Decode all rows from a data page.
    // Returns the number of rows decoded.
    int decode_page(const uint8_t* page_data, std::vector<Row>& out_rows) const;

    // Get the schema used for decoding
    const TableSchema& schema() const { return schema_; }

private:
    // Type-specific value extraction from the fixed-data region
    RowValue decode_fixed_column(const uint8_t* data, size_t data_len,
                                 const ColumnDef& col) const;

    // Type-specific value extraction from the variable-data region
    RowValue decode_variable_column(const uint8_t* data, size_t data_len,
                                    const ColumnDef& col) const;

    // Convert raw bytes to a typed RowValue
    RowValue bytes_to_value(const uint8_t* data, size_t len,
                            const ColumnDef& col) const;

    // UTF-16LE to UTF-8 conversion
    static std::string utf16le_to_utf8(const uint8_t* data, size_t byte_len);

    // DECIMAL/NUMERIC conversion
    static SqlDecimal decode_decimal(const uint8_t* data, size_t len,
                                     uint8_t precision, uint8_t scale);

    // UNIQUEIDENTIFIER conversion
    static SqlGuid decode_guid(const uint8_t* data);

    // DATETIME conversion
    static std::string decode_datetime(const uint8_t* data);
    static std::string decode_datetime2(const uint8_t* data, uint8_t scale);
    static std::string decode_smalldatetime(const uint8_t* data);
    static std::string decode_date(const uint8_t* data);
    static std::string decode_time(const uint8_t* data, size_t len, uint8_t scale);
    static std::string decode_datetimeoffset(const uint8_t* data, size_t len,
                                              uint8_t scale);

    const TableSchema& schema_;

    // Pre-computed: which columns are fixed vs variable
    std::vector<int> fixed_col_indices_;
    std::vector<int> var_col_indices_;
    std::vector<int> fixed_col_offsets_;  // byte offset within record for each fixed col
    int null_bitmap_bytes_ = 0;
};

}  // namespace bakread
