#include "bakread/parquet_writer.h"
#include "bakread/error.h"
#include "bakread/logging.h"

#ifdef BAKREAD_HAS_PARQUET
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/builder.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#endif

namespace bakread {

ParquetWriter::ParquetWriter() = default;
ParquetWriter::~ParquetWriter() {
    if (open_) close();
}

bool ParquetWriter::open(const std::string& path, const TableSchema& schema) {
#ifdef BAKREAD_HAS_PARQUET
    schema_ = schema;

    arrow_schema_ = build_arrow_schema(schema);
    if (!arrow_schema_) {
        throw ExportError("Failed to build Arrow schema");
    }

    auto result = arrow::io::FileOutputStream::Open(path);
    if (!result.ok()) {
        throw ExportError("Cannot open Parquet output: " + result.status().ToString());
    }
    output_stream_ = *result;

    // Configure Parquet writer with Snappy compression
    auto props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::SNAPPY)
        ->build();

    auto arrow_props = parquet::ArrowWriterProperties::Builder()
        .store_schema()
        ->build();

    auto writer_result = parquet::arrow::FileWriter::Open(
        *arrow_schema_, arrow::default_memory_pool(),
        output_stream_, props, arrow_props);

    if (!writer_result.ok()) {
        throw ExportError("Cannot create Parquet writer: " +
                          writer_result.status().ToString());
    }
    writer_ = std::move(*writer_result);

    // Create array builders for each column
    builders_.clear();
    for (auto& field : arrow_schema_->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto status = arrow::MakeBuilder(arrow::default_memory_pool(),
                                          field->type(), &builder);
        if (!status.ok()) {
            throw ExportError("Cannot create builder for column " +
                              field->name() + ": " + status.ToString());
        }
        builders_.push_back(std::shared_ptr<arrow::ArrayBuilder>(builder.release()));
    }

    open_ = true;
    current_batch_size_ = 0;

    LOG_INFO("Parquet writer opened: %s (%zu columns, Snappy compression)",
             path.c_str(), schema.columns.size());
    return true;
#else
    (void)path; (void)schema;
    throw ExportError("Parquet support not compiled. Install Apache Arrow and "
                      "rebuild with -DBAKREAD_ENABLE_PARQUET=ON");
#endif
}

bool ParquetWriter::write_row(const Row& row) {
#ifdef BAKREAD_HAS_PARQUET
    if (!open_) return false;

    for (size_t i = 0; i < row.size() && i < builders_.size(); ++i) {
        auto& builder = builders_[i];
        const auto& val = row[i];

        std::visit([&builder](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            arrow::Status s;

            if constexpr (std::is_same_v<T, NullValue>) {
                s = builder->AppendNull();
            }
            else if constexpr (std::is_same_v<T, bool>) {
                s = static_cast<arrow::BooleanBuilder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, int8_t>) {
                if (builder->type()->id() == arrow::Type::INT8)
                    s = static_cast<arrow::Int8Builder*>(builder.get())->Append(arg);
                else
                    s = static_cast<arrow::Int16Builder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, int16_t>) {
                s = static_cast<arrow::Int16Builder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                s = static_cast<arrow::Int32Builder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, int64_t>) {
                s = static_cast<arrow::Int64Builder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, float>) {
                s = static_cast<arrow::FloatBuilder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, double>) {
                s = static_cast<arrow::DoubleBuilder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, std::string>) {
                s = static_cast<arrow::StringBuilder*>(builder.get())->Append(arg);
            }
            else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                s = static_cast<arrow::BinaryBuilder*>(builder.get())->Append(
                    arg.data(), static_cast<int32_t>(arg.size()));
            }
            else if constexpr (std::is_same_v<T, SqlDecimal>) {
                s = static_cast<arrow::StringBuilder*>(builder.get())->Append(
                    arg.to_string());
            }
            else if constexpr (std::is_same_v<T, SqlGuid>) {
                s = static_cast<arrow::StringBuilder*>(builder.get())->Append(
                    arg.to_string());
            }
            else {
                s = builder->AppendNull();
            }
            (void)s;
        }, val);
    }

    ++rows_written_;
    ++current_batch_size_;

    if (current_batch_size_ >= BATCH_SIZE) {
        flush_batch();
    }

    return true;
#else
    (void)row;
    return false;
#endif
}

bool ParquetWriter::close() {
#ifdef BAKREAD_HAS_PARQUET
    if (!open_) return true;

    if (current_batch_size_ > 0) {
        flush_batch();
    }

    auto status = writer_->Close();
    if (!status.ok()) {
        LOG_ERROR("Error closing Parquet writer: %s", status.ToString().c_str());
    }

    (void)output_stream_->Close();
    open_ = false;

    LOG_INFO("Parquet writer closed: %llu rows written",
             (unsigned long long)rows_written_);
    return status.ok();
#else
    return true;
#endif
}

#ifdef BAKREAD_HAS_PARQUET
void ParquetWriter::flush_batch() {
    if (current_batch_size_ == 0) return;

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders_) {
        auto result = builder->Finish();
        if (!result.ok()) {
            LOG_ERROR("Failed to finalize array: %s", result.status().ToString().c_str());
            return;
        }
        arrays.push_back(*result);
    }

    auto batch = arrow::RecordBatch::Make(arrow_schema_, current_batch_size_, arrays);
    auto status = writer_->WriteRecordBatch(*batch);
    if (!status.ok()) {
        LOG_ERROR("Failed to write Parquet batch: %s", status.ToString().c_str());
    }

    current_batch_size_ = 0;
}

std::shared_ptr<arrow::Schema>
ParquetWriter::build_arrow_schema(const TableSchema& schema) {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (auto& col : schema.columns) {
        std::shared_ptr<arrow::DataType> type;

        switch (col.type) {
        case SqlType::Bit:
            type = arrow::boolean(); break;
        case SqlType::TinyInt:
            type = arrow::int8(); break;
        case SqlType::SmallInt:
            type = arrow::int16(); break;
        case SqlType::Int:
            type = arrow::int32(); break;
        case SqlType::BigInt:
            type = arrow::int64(); break;
        case SqlType::Real:
            type = arrow::float32(); break;
        case SqlType::Float:
            type = arrow::float64(); break;
        case SqlType::Money:
        case SqlType::SmallMoney:
            type = arrow::float64(); break;
        case SqlType::Decimal:
        case SqlType::Numeric:
            type = arrow::utf8(); break;  // Store as string for precision
        case SqlType::Binary:
        case SqlType::VarBinary:
        case SqlType::Image:
        case SqlType::Timestamp:
            type = arrow::binary(); break;
        default:
            type = arrow::utf8(); break;
        }

        fields.push_back(arrow::field(col.name, type, col.is_nullable));
    }

    return arrow::schema(fields);
}
#endif

// -------------------------------------------------------------------------
// Factory
// -------------------------------------------------------------------------
// (Defined in export_writer.h, implemented at bottom of this translation unit
//  alongside the JSON writer to avoid circular dependencies.)

}  // namespace bakread
