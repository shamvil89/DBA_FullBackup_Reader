#pragma once

#include <stdexcept>
#include <string>

namespace bakread {

// Base exception for all bakread errors
class BakReadError : public std::runtime_error {
public:
    explicit BakReadError(const std::string& msg)
        : std::runtime_error(msg) {}
};

class FileIOError : public BakReadError {
public:
    explicit FileIOError(const std::string& msg)
        : BakReadError("File I/O error: " + msg) {}
};

class BackupFormatError : public BakReadError {
public:
    explicit BackupFormatError(const std::string& msg)
        : BakReadError("Backup format error: " + msg) {}
};

class UnsupportedVersionError : public BakReadError {
public:
    explicit UnsupportedVersionError(const std::string& msg)
        : BakReadError("Unsupported SQL Server version: " + msg) {}
};

class CompressionError : public BakReadError {
public:
    explicit CompressionError(const std::string& msg)
        : BakReadError("Decompression error: " + msg) {}
};

class TdeError : public BakReadError {
public:
    explicit TdeError(const std::string& msg)
        : BakReadError("TDE/Encryption error: " + msg) {}
};

class OdbcError : public BakReadError {
public:
    explicit OdbcError(const std::string& msg)
        : BakReadError("ODBC error: " + msg) {}
};

class TableNotFoundError : public BakReadError {
public:
    explicit TableNotFoundError(const std::string& schema, const std::string& table)
        : BakReadError("Table not found: " + schema + "." + table) {}
};

class PageCorruptionError : public BakReadError {
public:
    explicit PageCorruptionError(int32_t file_id, int32_t page_id,
                                  const std::string& detail)
        : BakReadError("Page corruption at (" + std::to_string(file_id) + ":" +
                        std::to_string(page_id) + "): " + detail) {}
};

class ExportError : public BakReadError {
public:
    explicit ExportError(const std::string& msg)
        : BakReadError("Export error: " + msg) {}
};

class ConfigError : public BakReadError {
public:
    explicit ConfigError(const std::string& msg)
        : BakReadError("Configuration error: " + msg) {}
};

}  // namespace bakread
