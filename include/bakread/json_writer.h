#pragma once

#include "bakread/export_writer.h"

#include <fstream>
#include <string>

namespace bakread {

// JSON Lines writer -- one JSON object per line
class JsonWriter : public IExportWriter {
public:
    JsonWriter();
    ~JsonWriter() override;

    bool open(const std::string& path, const TableSchema& schema) override;
    bool write_row(const Row& row) override;
    bool close() override;
    uint64_t rows_written() const override { return rows_written_; }

private:
    std::string format_value(const RowValue& val) const;
    static std::string escape_json(const std::string& s);

    std::ofstream file_;
    TableSchema   schema_;
    uint64_t      rows_written_ = 0;
    bool          open_ = false;
};

}  // namespace bakread
