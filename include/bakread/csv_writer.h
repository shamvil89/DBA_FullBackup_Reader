#pragma once

#include "bakread/export_writer.h"

#include <fstream>
#include <string>

namespace bakread {

class CsvWriter : public IExportWriter {
public:
    explicit CsvWriter(const std::string& delimiter = ",");
    ~CsvWriter() override;

    bool open(const std::string& path, const TableSchema& schema) override;
    bool write_row(const Row& row) override;
    bool close() override;
    uint64_t rows_written() const override { return rows_written_; }

private:
    std::string format_value(const RowValue& val) const;
    std::string escape_csv(const std::string& s) const;

    std::string   delimiter_;
    std::ofstream file_;
    TableSchema   schema_;
    uint64_t      rows_written_ = 0;
    bool          open_ = false;
};

}  // namespace bakread
