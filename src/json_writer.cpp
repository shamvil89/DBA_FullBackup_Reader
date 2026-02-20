#include "bakread/json_writer.h"
#include "bakread/csv_writer.h"
#include "bakread/parquet_writer.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <iomanip>
#include <sstream>

namespace bakread {

JsonWriter::JsonWriter() = default;

JsonWriter::~JsonWriter() {
    if (open_) close();
}

bool JsonWriter::open(const std::string& path, const TableSchema& schema) {
    schema_ = schema;

    file_.open(path, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!file_.is_open()) {
        throw ExportError("Cannot open output file: " + path);
    }

    open_ = true;
    LOG_INFO("JSON Lines writer opened: %s (%zu columns)",
             path.c_str(), schema.columns.size());
    return true;
}

bool JsonWriter::write_row(const Row& row) {
    if (!open_) return false;

    file_ << "{";
    for (size_t i = 0; i < row.size() && i < schema_.columns.size(); ++i) {
        if (i > 0) file_ << ",";
        file_ << "\"" << escape_json(schema_.columns[i].name) << "\":"
              << format_value(row[i]);
    }
    file_ << "}\n";

    ++rows_written_;

    if (rows_written_ % 50000 == 0) {
        file_.flush();
    }

    return file_.good();
}

bool JsonWriter::close() {
    if (!open_) return true;

    file_.flush();
    file_.close();
    open_ = false;

    LOG_INFO("JSON Lines writer closed: %llu rows written",
             (unsigned long long)rows_written_);
    return true;
}

std::string JsonWriter::format_value(const RowValue& val) const {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, NullValue>) {
            return "null";
        }
        else if constexpr (std::is_same_v<T, bool>) {
            return arg ? "true" : "false";
        }
        else if constexpr (std::is_same_v<T, int8_t>) {
            return std::to_string(static_cast<int>(arg));
        }
        else if constexpr (std::is_same_v<T, int16_t>) {
            return std::to_string(arg);
        }
        else if constexpr (std::is_same_v<T, int32_t>) {
            return std::to_string(arg);
        }
        else if constexpr (std::is_same_v<T, int64_t>) {
            return std::to_string(arg);
        }
        else if constexpr (std::is_same_v<T, float>) {
            std::ostringstream oss;
            oss << std::setprecision(7) << arg;
            return oss.str();
        }
        else if constexpr (std::is_same_v<T, double>) {
            std::ostringstream oss;
            oss << std::setprecision(15) << arg;
            return oss.str();
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            return "\"" + escape_json(arg) + "\"";
        }
        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            std::ostringstream oss;
            oss << "\"0x";
            for (auto b : arg)
                oss << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(b);
            oss << "\"";
            return oss.str();
        }
        else if constexpr (std::is_same_v<T, SqlDecimal>) {
            return arg.to_string();
        }
        else if constexpr (std::is_same_v<T, SqlGuid>) {
            return "\"" + arg.to_string() + "\"";
        }
        else {
            return "null";
        }
    }, val);
}

std::string JsonWriter::escape_json(const std::string& s) {
    std::string result;
    result.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
        case '"':  result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\b': result += "\\b";  break;
        case '\f': result += "\\f";  break;
        case '\n': result += "\\n";  break;
        case '\r': result += "\\r";  break;
        case '\t': result += "\\t";  break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                char buf[8];
                snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                result += buf;
            } else {
                result += c;
            }
        }
    }
    return result;
}

// =========================================================================
// Writer factory -- create_writer()
// =========================================================================

std::unique_ptr<IExportWriter> create_writer(OutputFormat format,
                                              const std::string& delimiter) {
    switch (format) {
    case OutputFormat::CSV:
        return std::make_unique<CsvWriter>(delimiter);
    case OutputFormat::Parquet:
        return std::make_unique<ParquetWriter>();
    case OutputFormat::JSONL:
        return std::make_unique<JsonWriter>();
    default:
        throw ExportError("Unknown output format");
    }
}

}  // namespace bakread
