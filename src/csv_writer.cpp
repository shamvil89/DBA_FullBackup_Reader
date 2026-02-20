#include "bakread/csv_writer.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#include <iomanip>
#include <sstream>

namespace bakread {

CsvWriter::CsvWriter(const std::string& delimiter)
    : delimiter_(delimiter)
{
}

CsvWriter::~CsvWriter() {
    if (open_) close();
}

bool CsvWriter::open(const std::string& path, const TableSchema& schema) {
    schema_ = schema;

    file_.open(path, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!file_.is_open()) {
        throw ExportError("Cannot open output file: " + path);
    }

    // Write UTF-8 BOM for Excel compatibility
    file_.write("\xEF\xBB\xBF", 3);

    // Write header row
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (i > 0) file_ << delimiter_;
        file_ << escape_csv(schema.columns[i].name);
    }
    file_ << "\r\n";

    open_ = true;
    LOG_INFO("CSV writer opened: %s (%zu columns)", path.c_str(),
             schema.columns.size());
    return true;
}

bool CsvWriter::write_row(const Row& row) {
    if (!open_) return false;

    for (size_t i = 0; i < row.size() && i < schema_.columns.size(); ++i) {
        if (i > 0) file_ << delimiter_;
        file_ << format_value(row[i]);
    }
    file_ << "\r\n";

    ++rows_written_;

    // Periodic flush for crash safety on large exports
    if (rows_written_ % 50000 == 0) {
        file_.flush();
    }

    return file_.good();
}

bool CsvWriter::close() {
    if (!open_) return true;

    file_.flush();
    file_.close();
    open_ = false;

    LOG_INFO("CSV writer closed: %llu rows written",
             (unsigned long long)rows_written_);
    return true;
}

std::string CsvWriter::format_value(const RowValue& val) const {
    return std::visit([this](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<T, NullValue>) {
            return "";
        }
        else if constexpr (std::is_same_v<T, bool>) {
            return arg ? "1" : "0";
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
            return escape_csv(arg);
        }
        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            // Binary data as hex string
            std::ostringstream oss;
            oss << "0x";
            for (auto b : arg)
                oss << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(b);
            return oss.str();
        }
        else if constexpr (std::is_same_v<T, SqlDecimal>) {
            return arg.to_string();
        }
        else if constexpr (std::is_same_v<T, SqlGuid>) {
            return arg.to_string();
        }
        else {
            return "";
        }
    }, val);
}

std::string CsvWriter::escape_csv(const std::string& s) const {
    bool needs_quoting = false;
    for (char c : s) {
        if (c == '"' || c == '\n' || c == '\r' || c == delimiter_[0]) {
            needs_quoting = true;
            break;
        }
    }

    if (!needs_quoting) return s;

    std::string result = "\"";
    for (char c : s) {
        if (c == '"') result += "\"\"";
        else result += c;
    }
    result += "\"";
    return result;
}

}  // namespace bakread
