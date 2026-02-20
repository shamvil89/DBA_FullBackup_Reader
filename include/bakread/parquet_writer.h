#pragma once

#include "bakread/export_writer.h"

#include <memory>
#include <string>

#ifdef BAKREAD_HAS_PARQUET
namespace arrow { class Schema; class ArrayBuilder; class RecordBatch; }
namespace arrow::io { class FileOutputStream; }
namespace parquet::arrow { class FileWriter; }
#endif

namespace bakread {

class ParquetWriter : public IExportWriter {
public:
    ParquetWriter();
    ~ParquetWriter() override;

    bool open(const std::string& path, const TableSchema& schema) override;
    bool write_row(const Row& row) override;
    bool close() override;
    uint64_t rows_written() const override { return rows_written_; }

private:
#ifdef BAKREAD_HAS_PARQUET
    void flush_batch();

    // Map SQL types to Arrow types
    std::shared_ptr<arrow::Schema> build_arrow_schema(const TableSchema& schema);

    std::shared_ptr<arrow::io::FileOutputStream> output_stream_;
    std::unique_ptr<parquet::arrow::FileWriter>  writer_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    std::shared_ptr<arrow::Schema> arrow_schema_;

    static constexpr int BATCH_SIZE = 65536;
    int current_batch_size_ = 0;
#endif

    TableSchema schema_;
    uint64_t    rows_written_ = 0;
    bool        open_ = false;
};

}  // namespace bakread
