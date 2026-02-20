#pragma once

#include "bakread/types.h"

#include <cstdint>
#include <memory>
#include <string>

namespace bakread {

// -------------------------------------------------------------------------
// IExportWriter -- common interface for all output formats
// -------------------------------------------------------------------------
class IExportWriter {
public:
    virtual ~IExportWriter() = default;

    // Open the output file and write header/schema info
    virtual bool open(const std::string& path, const TableSchema& schema) = 0;

    // Write a single row
    virtual bool write_row(const Row& row) = 0;

    // Flush buffered data and close the file
    virtual bool close() = 0;

    // Get the number of rows written
    virtual uint64_t rows_written() const = 0;
};

// Factory function to create the appropriate writer based on format
std::unique_ptr<IExportWriter> create_writer(OutputFormat format,
                                              const std::string& delimiter = ",");

}  // namespace bakread
