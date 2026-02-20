#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace bakread {

// -------------------------------------------------------------------------
// SQL Server data type identifiers (matches sys.types.system_type_id)
// -------------------------------------------------------------------------
enum class SqlType : uint8_t {
    Unknown       = 0,
    TinyInt       = 48,
    SmallInt      = 52,
    Int           = 56,
    BigInt        = 127,
    Bit           = 104,
    Float         = 62,
    Real          = 59,
    Decimal       = 106,
    Numeric       = 108,
    Money         = 60,
    SmallMoney    = 122,
    Date          = 40,
    Time          = 41,
    DateTime      = 61,
    DateTime2     = 42,
    SmallDateTime = 58,
    DateTimeOffset= 43,
    Char          = 175,
    VarChar       = 167,
    NChar         = 239,
    NVarChar      = 231,
    Text          = 35,
    NText         = 99,
    Binary        = 173,
    VarBinary     = 165,
    Image         = 34,
    UniqueId      = 36,
    Xml           = 241,
    Timestamp     = 189,
    Sql_Variant   = 98,
};

inline bool is_fixed_length(SqlType t) {
    switch (t) {
        case SqlType::TinyInt:
        case SqlType::SmallInt:
        case SqlType::Int:
        case SqlType::BigInt:
        case SqlType::Bit:
        case SqlType::Float:
        case SqlType::Real:
        case SqlType::Money:
        case SqlType::SmallMoney:
        case SqlType::Date:
        case SqlType::Time:
        case SqlType::DateTime:
        case SqlType::DateTime2:
        case SqlType::DateTimeOffset:
        case SqlType::SmallDateTime:
        case SqlType::UniqueId:
        case SqlType::Timestamp:
            return true;
        case SqlType::Decimal:
        case SqlType::Numeric:
        case SqlType::Char:
        case SqlType::NChar:
        case SqlType::Binary:
            return true;
        default:
            return false;
    }
}

inline bool is_unicode(SqlType t) {
    return t == SqlType::NChar || t == SqlType::NVarChar || t == SqlType::NText;
}

inline bool is_lob(SqlType t) {
    return t == SqlType::Text || t == SqlType::NText || t == SqlType::Image ||
           t == SqlType::Xml;
}

// -------------------------------------------------------------------------
// Column definition
// -------------------------------------------------------------------------
struct ColumnDef {
    int32_t     column_id    = 0;
    std::string name;
    SqlType     type         = SqlType::Unknown;
    int16_t     max_length   = 0;
    uint8_t     precision    = 0;
    uint8_t     scale        = 0;
    bool        is_nullable  = true;
    bool        is_identity  = false;
    bool        is_computed  = false;
    int32_t     leaf_offset  = 0;   // physical offset in fixed-data region
};

// -------------------------------------------------------------------------
// Runtime value representation
// -------------------------------------------------------------------------
using NullValue = std::monostate;

struct SqlDecimal {
    bool    positive = true;
    uint8_t precision = 18;
    uint8_t scale     = 0;
    uint8_t data[16]  = {};

    double to_double() const;
    std::string to_string() const;
};

struct SqlGuid {
    uint8_t bytes[16] = {};
    std::string to_string() const;
};

using RowValue = std::variant<
    NullValue,
    bool,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double,
    std::string,            // UTF-8 encoded text / binary as hex
    std::vector<uint8_t>,   // raw binary
    SqlDecimal,
    SqlGuid
>;

using Row = std::vector<RowValue>;

// -------------------------------------------------------------------------
// Table schema
// -------------------------------------------------------------------------
struct TableSchema {
    int32_t     object_id     = 0;
    std::string schema_name;
    std::string table_name;
    std::vector<ColumnDef> columns;
    bool        is_heap       = true;  // no clustered index
    int32_t     partition_count = 1;

    std::string qualified_name() const {
        return schema_name + "." + table_name;
    }
};

// -------------------------------------------------------------------------
// Backup metadata
// -------------------------------------------------------------------------
enum class BackupType : uint8_t {
    Unknown       = 0,
    Full          = 1,
    Differential  = 2,
    Log           = 3,
};

struct BackupSetInfo {
    int32_t     position         = 0;
    std::string database_name;
    std::string server_name;
    BackupType  backup_type      = BackupType::Unknown;
    int32_t     compatibility_level = 0;
    bool        is_compressed    = false;
    bool        is_encrypted     = false;
    bool        is_tde           = false;
    uint64_t    backup_size      = 0;
    uint64_t    compressed_size  = 0;
    std::string backup_start_date;
    std::string backup_finish_date;
    int32_t     software_major   = 0;
    int32_t     software_minor   = 0;
};

struct BackupFileInfo {
    std::string logical_name;
    std::string physical_name;
    char        file_type = 'D';  // D=data, L=log
    int64_t     size      = 0;
    int32_t     file_id   = 0;
};

struct BackupInfo {
    std::string                  file_path;
    std::vector<BackupSetInfo>   backup_sets;
    std::vector<BackupFileInfo>  file_list;
};

// -------------------------------------------------------------------------
// SQL Server page identification
// -------------------------------------------------------------------------
struct PageId {
    int32_t file_id = 0;
    int32_t page_id = 0;

    bool operator==(const PageId& o) const {
        return file_id == o.file_id && page_id == o.page_id;
    }
    bool is_null() const { return file_id == 0 && page_id == 0; }
};

// -------------------------------------------------------------------------
// Execution mode
// -------------------------------------------------------------------------
enum class ExecMode {
    Auto,
    Direct,
    Restore,
};

// -------------------------------------------------------------------------
// Output format
// -------------------------------------------------------------------------
enum class OutputFormat {
    CSV,
    Parquet,
    JSONL,
};

// -------------------------------------------------------------------------
// Progress callback
// -------------------------------------------------------------------------
struct Progress {
    uint64_t bytes_processed  = 0;
    uint64_t bytes_total      = 0;
    uint64_t rows_exported    = 0;
    double   pct              = 0.0;
};

using ProgressCallback = std::function<void(const Progress&)>;

}  // namespace bakread
