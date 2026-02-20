#include "bakread/row_decoder.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <sstream>
#include <iomanip>

namespace bakread {

// -------------------------------------------------------------------------
// Proleptic Gregorian calendar: days since 0001-01-01 -> (year, month, day)
// Uses the algorithm from Howard Hinnant's date library.
// -------------------------------------------------------------------------
static void days_to_ymd(int days, int& y, int& m, int& d) {
    days += 305;  // shift epoch from 0001-01-01 to 0000-03-01
    int era = (days >= 0 ? days : days - 146096) / 146097;
    int doe = days - era * 146097;
    int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = yoe + era * 400 + 1;
    int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    if (m <= 2) ++y;
}

// -------------------------------------------------------------------------
// SqlDecimal helpers
// -------------------------------------------------------------------------

double SqlDecimal::to_double() const {
    // Convert the 128-bit integer value to double, then apply scale
    double val = 0.0;
    double multiplier = 1.0;
    for (int i = 0; i < 16; ++i) {
        val += data[i] * multiplier;
        multiplier *= 256.0;
    }
    val /= std::pow(10.0, scale);
    return positive ? val : -val;
}

std::string SqlDecimal::to_string() const {
    // Simple string representation via double (sufficient for most cases)
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(scale) << to_double();
    return oss.str();
}

// -------------------------------------------------------------------------
// SqlGuid helpers
// -------------------------------------------------------------------------

std::string SqlGuid::to_string() const {
    // SQL Server stores GUIDs in mixed-endian format:
    //   Data1 (4 bytes LE), Data2 (2 bytes LE), Data3 (2 bytes LE),
    //   Data4 (8 bytes BE)
    char buf[64];
    snprintf(buf, sizeof(buf),
             "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
             bytes[3], bytes[2], bytes[1], bytes[0],
             bytes[5], bytes[4],
             bytes[7], bytes[6],
             bytes[8], bytes[9],
             bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    return buf;
}

// -------------------------------------------------------------------------
// RowDecoder
// -------------------------------------------------------------------------

RowDecoder::RowDecoder(const TableSchema& schema)
    : schema_(schema)
{
    int cur_offset = 4;  // fixed data starts after the 4-byte record header
    for (int i = 0; i < static_cast<int>(schema_.columns.size()); ++i) {
        auto& col = schema_.columns[i];
        if (is_fixed_length(col.type) && !is_lob(col.type)) {
            fixed_col_indices_.push_back(i);
            int off = (col.leaf_offset > 0) ? col.leaf_offset : cur_offset;
            fixed_col_offsets_.push_back(off);
            cur_offset = off + col.max_length;
        } else {
            var_col_indices_.push_back(i);
        }
    }
    null_bitmap_bytes_ = (static_cast<int>(schema_.columns.size()) + 7) / 8;
}

int RowDecoder::decode_page(const uint8_t* page_data,
                             std::vector<Row>& out_rows) const {
    PageHeader hdr;
    std::memcpy(&hdr, page_data, sizeof(hdr));

    if (hdr.type != static_cast<uint8_t>(PageType::Data)) {
        return 0;
    }

    int decoded = 0;
    for (int slot = 0; slot < hdr.slot_count; ++slot) {
        uint16_t offset = get_slot_offset(page_data, slot);
        if (offset < PAGE_HEADER_SIZE || offset >= PAGE_SIZE - 2) {
            LOG_DEBUG("Invalid slot offset %u for slot %d on page %u:%u",
                      offset, slot, hdr.this_file, hdr.this_page);
            continue;
        }

        // Check status byte for ghost/forwarding records to skip
        uint8_t status_a = page_data[offset];
        uint8_t rec_type = status_a & RecordStatus::TypeMask;
        if (rec_type == RecordStatus::ForwardingStub) continue;

        Row row;
        if (decode_row(page_data, offset, row)) {
            out_rows.push_back(std::move(row));
            ++decoded;
        }
    }
    return decoded;
}

bool RowDecoder::decode_row(const uint8_t* page_data, uint16_t record_offset,
                             Row& out_row) const {
    const uint8_t* rec = page_data + record_offset;
    size_t max_len = PAGE_SIZE - record_offset;

    if (max_len < 4) return false;

    uint8_t status_a = rec[0];
    // uint8_t status_b = rec[1];  // Available for future use

    bool has_null_bitmap = (status_a & RecordStatus::HasNullBitmap) != 0;
    bool has_var_cols    = (status_a & RecordStatus::HasVarColumns) != 0;

    // Fixed-length data end offset
    uint16_t fixed_end;
    std::memcpy(&fixed_end, rec + 2, 2);

    if (fixed_end > max_len) {
        LOG_DEBUG("Fixed data end offset %u exceeds record bounds", fixed_end);
        return false;
    }

    // -- Parse null bitmap --
    // The null bitmap area starts with a 2-byte column count,
    // followed by ceil(column_count/8) bitmap bytes.
    std::vector<bool> null_bits(schema_.columns.size(), false);
    size_t bitmap_start = fixed_end;
    size_t null_area_size = 0;

    if (has_null_bitmap && bitmap_start + 2 <= max_len) {
        uint16_t rec_col_count;
        std::memcpy(&rec_col_count, rec + bitmap_start, 2);
        int null_bmp_bytes = (rec_col_count + 7) / 8;
        null_area_size = 2 + null_bmp_bytes;

        if (bitmap_start + null_area_size <= max_len) {
            for (size_t col = 0; col < schema_.columns.size() && col < rec_col_count; ++col) {
                size_t byte_idx = col / 8;
                size_t bit_idx  = col % 8;
                if (rec[bitmap_start + 2 + byte_idx] & (1 << bit_idx)) {
                    null_bits[col] = true;
                }
            }
        }
    }

    // -- Parse variable-length column offset array --
    size_t var_offset_start = bitmap_start + null_area_size;
    uint16_t var_col_count = 0;
    std::vector<uint16_t> var_end_offsets;

    if (has_var_cols && var_offset_start + 2 <= max_len) {
        std::memcpy(&var_col_count, rec + var_offset_start, 2);
        var_offset_start += 2;

        var_end_offsets.resize(var_col_count);
        for (uint16_t v = 0; v < var_col_count; ++v) {
            if (var_offset_start + 2 > max_len) break;
            std::memcpy(&var_end_offsets[v], rec + var_offset_start, 2);
            var_offset_start += 2;
        }
    }

    // -- Decode columns --
    out_row.resize(schema_.columns.size());

    // Fixed-length columns
    for (size_t fi = 0; fi < fixed_col_indices_.size(); ++fi) {
        int ci = fixed_col_indices_[fi];
        auto& col = schema_.columns[ci];

        if (null_bits[ci]) {
            out_row[ci] = NullValue{};
            continue;
        }

        int data_offset = fixed_col_offsets_[fi];

        if (data_offset < 4) data_offset = 4;
        if (static_cast<size_t>(data_offset) >= fixed_end) {
            out_row[ci] = NullValue{};
            continue;
        }

        size_t avail = fixed_end - data_offset;
        out_row[ci] = decode_fixed_column(rec + data_offset,
                                           std::min<size_t>(avail, col.max_length),
                                           col);
    }

    // Variable-length columns
    uint16_t var_data_start_offset = static_cast<uint16_t>(var_offset_start);
    for (size_t vi = 0; vi < var_col_indices_.size(); ++vi) {
        int ci = var_col_indices_[vi];
        auto& col = schema_.columns[ci];

        if (null_bits[ci]) {
            out_row[ci] = NullValue{};
            continue;
        }

        if (vi >= var_end_offsets.size()) {
            out_row[ci] = NullValue{};
            continue;
        }

        uint16_t end_off = var_end_offsets[vi];
        uint16_t start_off = (vi == 0) ? var_data_start_offset
                                       : var_end_offsets[vi - 1];

        // High bit of end_off may indicate a complex column (LOB pointer)
        bool is_complex = (end_off & 0x8000) != 0;
        end_off &= 0x7FFF;

        if (is_complex) {
            // LOB/overflow pointer -- we cannot resolve this in direct mode
            // without following the pointer chain
            out_row[ci] = std::string("[LOB data]");
            continue;
        }

        if (start_off >= end_off || end_off > max_len) {
            out_row[ci] = NullValue{};
            continue;
        }

        out_row[ci] = decode_variable_column(rec + start_off,
                                              end_off - start_off, col);
    }

    return true;
}

RowValue RowDecoder::decode_fixed_column(const uint8_t* data, size_t data_len,
                                          const ColumnDef& col) const {
    return bytes_to_value(data, data_len, col);
}

RowValue RowDecoder::decode_variable_column(const uint8_t* data, size_t data_len,
                                             const ColumnDef& col) const {
    return bytes_to_value(data, data_len, col);
}

RowValue RowDecoder::bytes_to_value(const uint8_t* data, size_t len,
                                     const ColumnDef& col) const {
    if (len == 0) return NullValue{};

    switch (col.type) {
    case SqlType::TinyInt:
        if (len < 1) return NullValue{};
        return static_cast<int8_t>(data[0]);

    case SqlType::SmallInt:
        if (len < 2) return NullValue{};
        { int16_t v; std::memcpy(&v, data, 2); return v; }

    case SqlType::Int:
        if (len < 4) return NullValue{};
        { int32_t v; std::memcpy(&v, data, 4); return v; }

    case SqlType::BigInt:
        if (len < 8) return NullValue{};
        { int64_t v; std::memcpy(&v, data, 8); return v; }

    case SqlType::Bit:
        return static_cast<bool>(data[0] != 0);

    case SqlType::Real:
        if (len < 4) return NullValue{};
        { float v; std::memcpy(&v, data, 4); return v; }

    case SqlType::Float:
        if (len < 8) return NullValue{};
        { double v; std::memcpy(&v, data, 8); return v; }

    case SqlType::Money:
        if (len < 8) return NullValue{};
        {
            int32_t hi; std::memcpy(&hi, data, 4);
            int32_t lo; std::memcpy(&lo, data + 4, 4);
            int64_t combined = (static_cast<int64_t>(hi) << 32) | static_cast<uint32_t>(lo);
            return static_cast<double>(combined) / 10000.0;
        }

    case SqlType::SmallMoney:
        if (len < 4) return NullValue{};
        { int32_t v; std::memcpy(&v, data, 4); return static_cast<double>(v) / 10000.0; }

    case SqlType::Decimal:
    case SqlType::Numeric:
        return decode_decimal(data, len, col.precision, col.scale);

    case SqlType::Char:
    case SqlType::VarChar:
    case SqlType::Text:
        return std::string(reinterpret_cast<const char*>(data), len);

    case SqlType::NChar:
    case SqlType::NVarChar:
    case SqlType::NText:
        return utf16le_to_utf8(data, len);

    case SqlType::Binary:
    case SqlType::VarBinary:
    case SqlType::Image:
    case SqlType::Timestamp:
        return std::vector<uint8_t>(data, data + len);

    case SqlType::UniqueId:
        if (len < 16) return NullValue{};
        return decode_guid(data);

    case SqlType::Date:
        if (len < 3) return NullValue{};
        return decode_date(data);

    case SqlType::DateTime:
        if (len < 8) return NullValue{};
        return decode_datetime(data);

    case SqlType::SmallDateTime:
        if (len < 4) return NullValue{};
        return decode_smalldatetime(data);

    case SqlType::DateTime2:
        return decode_datetime2(data, col.scale);

    case SqlType::Time:
        return decode_time(data, len, col.scale);

    case SqlType::DateTimeOffset:
        return decode_datetimeoffset(data, len, col.scale);

    default:
        return std::vector<uint8_t>(data, data + len);
    }
}

std::string RowDecoder::utf16le_to_utf8(const uint8_t* data, size_t byte_len) {
    std::string result;
    result.reserve(byte_len);

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
            // Handle surrogate pairs for characters outside BMP
            if (ch >= 0xD800 && ch <= 0xDBFF && i + 3 < byte_len) {
                uint16_t low = static_cast<uint16_t>(data[i+2]) |
                               (static_cast<uint16_t>(data[i+3]) << 8);
                if (low >= 0xDC00 && low <= 0xDFFF) {
                    uint32_t cp = 0x10000 + ((ch - 0xD800) << 10) + (low - 0xDC00);
                    result.push_back(static_cast<char>(0xF0 | (cp >> 18)));
                    result.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                    result.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                    result.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
                    i += 2;
                    continue;
                }
            }
            result.push_back(static_cast<char>(0xE0 | (ch >> 12)));
            result.push_back(static_cast<char>(0x80 | ((ch >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
        }
    }
    return result;
}

SqlDecimal RowDecoder::decode_decimal(const uint8_t* data, size_t len,
                                       uint8_t precision, uint8_t scale) {
    SqlDecimal dec;
    dec.precision = precision;
    dec.scale     = scale;

    if (len < 1) return dec;

    dec.positive = (data[0] != 0);

    // The integer value follows the sign byte, stored in 4/8/12/16 bytes
    // depending on precision
    size_t int_bytes = len - 1;
    if (int_bytes > 16) int_bytes = 16;

    std::memcpy(dec.data, data + 1, int_bytes);
    return dec;
}

SqlGuid RowDecoder::decode_guid(const uint8_t* data) {
    SqlGuid guid;
    std::memcpy(guid.bytes, data, 16);
    return guid;
}

std::string RowDecoder::decode_datetime(const uint8_t* data) {
    // DATETIME: 4 bytes (days since 1900-01-01) + 4 bytes (1/300 sec ticks)
    int32_t days, ticks;
    std::memcpy(&days, data, 4);
    std::memcpy(&ticks, data + 4, 4);

    // Convert days to date (simplified)
    struct tm base = {};
    base.tm_year = 0;   // 1900
    base.tm_mon  = 0;   // January
    base.tm_mday = 1 + days;
    mktime(&base);

    int total_seconds = ticks / 300;
    int hours   = total_seconds / 3600;
    int minutes = (total_seconds % 3600) / 60;
    int seconds = total_seconds % 60;
    int millis  = (ticks % 300) * 10 / 3;

    char buf[64];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
             base.tm_year + 1900, base.tm_mon + 1, base.tm_mday,
             hours, minutes, seconds, millis);
    return buf;
}

std::string RowDecoder::decode_datetime2(const uint8_t* data, uint8_t scale) {
    // DATETIME2 packs time and date into variable-length representation
    // Time: first N bytes (3-5 depending on scale), Date: last 3 bytes
    if (scale > 7) scale = 7;

    int time_bytes = (scale <= 2) ? 3 : (scale <= 4) ? 4 : 5;

    // Date: last 3 bytes (days since 0001-01-01)
    uint32_t date_val = 0;
    std::memcpy(&date_val, data + time_bytes, 3);
    date_val &= 0x00FFFFFF;

    // Time: first N bytes (fractional time units)
    uint64_t time_val = 0;
    std::memcpy(&time_val, data, time_bytes);
    if (time_bytes == 3) time_val &= 0x00FFFFFF;
    else if (time_bytes == 4) time_val &= 0x00FFFFFFFF;

    int y, m, d;
    days_to_ymd(static_cast<int>(date_val), y, m, d);

    // Convert time_val to HH:MM:SS.fraction
    static const uint64_t scales[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};
    uint64_t ticks_per_sec = scales[scale];
    uint64_t total_secs = time_val / ticks_per_sec;
    uint64_t frac = time_val % ticks_per_sec;

    int hours   = static_cast<int>(total_secs / 3600);
    int minutes = static_cast<int>((total_secs % 3600) / 60);
    int seconds = static_cast<int>(total_secs % 60);

    char buf[64];
    if (scale > 0) {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%0*llu",
                 y, m, d, hours, minutes, seconds,
                 scale, (unsigned long long)frac);
    } else {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                 y, m, d, hours, minutes, seconds);
    }
    return buf;
}

std::string RowDecoder::decode_smalldatetime(const uint8_t* data) {
    // SMALLDATETIME: 2 bytes (days since 1900-01-01) + 2 bytes (minutes)
    uint16_t days, minutes;
    std::memcpy(&days, data, 2);
    std::memcpy(&minutes, data + 2, 2);

    struct tm base = {};
    base.tm_year = 0;
    base.tm_mon  = 0;
    base.tm_mday = 1 + days;
    mktime(&base);

    int hours = minutes / 60;
    int mins  = minutes % 60;

    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:00",
             base.tm_year + 1900, base.tm_mon + 1, base.tm_mday,
             hours, mins);
    return buf;
}

std::string RowDecoder::decode_date(const uint8_t* data) {
    // DATE: 3 bytes (days since 0001-01-01)
    uint32_t date_val = 0;
    std::memcpy(&date_val, data, 3);
    date_val &= 0x00FFFFFF;

    int y, m, d;
    days_to_ymd(static_cast<int>(date_val), y, m, d);

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
    return buf;
}

std::string RowDecoder::decode_time(const uint8_t* data, size_t len, uint8_t scale) {
    if (scale > 7) scale = 7;
    int time_bytes = (scale <= 2) ? 3 : (scale <= 4) ? 4 : 5;
    if (static_cast<int>(len) < time_bytes) return "";

    uint64_t time_val = 0;
    std::memcpy(&time_val, data, time_bytes);
    if (time_bytes == 3) time_val &= 0x00FFFFFF;
    else if (time_bytes == 4) time_val &= 0xFFFFFFFF;

    static const uint64_t scales[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};
    uint64_t ticks_per_sec = scales[scale];
    uint64_t total_secs = time_val / ticks_per_sec;
    uint64_t frac = time_val % ticks_per_sec;

    int hours   = static_cast<int>(total_secs / 3600);
    int minutes = static_cast<int>((total_secs % 3600) / 60);
    int seconds = static_cast<int>(total_secs % 60);

    char buf[32];
    if (scale > 0) {
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%0*llu",
                 hours, minutes, seconds,
                 scale, (unsigned long long)frac);
    } else {
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hours, minutes, seconds);
    }
    return buf;
}

std::string RowDecoder::decode_datetimeoffset(const uint8_t* data, size_t len,
                                               uint8_t scale) {
    if (scale > 7) scale = 7;
    int time_bytes = (scale <= 2) ? 3 : (scale <= 4) ? 4 : 5;
    int total_needed = time_bytes + 3 + 2;  // time + date + offset
    if (static_cast<int>(len) < total_needed) return "";

    std::string dt = decode_datetime2(data, scale);

    int16_t tz_offset;
    std::memcpy(&tz_offset, data + time_bytes + 3, 2);
    int tz_hours = tz_offset / 60;
    int tz_mins  = std::abs(tz_offset % 60);

    char buf[8];
    snprintf(buf, sizeof(buf), "%+03d:%02d", tz_hours, tz_mins);
    return dt + buf;
}

}  // namespace bakread
