/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/duckdb_reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "arrow/io/api.h"
// #include "arrow/util/type_fwd.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/reader.h"
#include <arrow/table.h> // @manual

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/tpch/gen/TpchGen.h"
#include <unistd.h>       // for syscall()
#include <sys/syscall.h> 
#include <gflags/gflags.h>

DEFINE_string(path_to_file, "path", "path to file");
DEFINE_string(output_path, "path", "path to file");
DEFINE_string(gzip_format, "zlib", "Data format");
using std::chrono::system_clock;
using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

const uint32_t kNumRowsPerBatch = 60000;
const uint32_t kNumBatches = 50;
const uint32_t kNumRowsPerRowGroup = 60000;
const double kFilterErrorMargin = 0.2;

class ParquetTransferBenchmark {
 public:
  explicit ParquetTransferBenchmark(bool disableDictionary)
      : disableDictionary_(disableDictionary) {
  }

  ~ParquetTransferBenchmark() {
    // writer_->close();
  }

  arrow::Result<int> writeToFile1() {
    // Configure general Parquet reader settings
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    const int64_t batch_size = 60000;

    /*-------------batch reader----------------------*/
    ::parquet::ArrowReaderProperties properties = ::parquet::default_arrow_reader_properties();
    properties.set_batch_size(batch_size);
    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    if (arrow::Status::OK() != ::parquet::arrow::FileReader::Make(pool,
                ::parquet::ParquetFileReader::OpenFile(FLAGS_path_to_file, false), properties, &parquet_reader)) {
        std::cout << "error" << std::endl;
        return -1;
    }
    if (arrow::Status::OK() != parquet_reader->GetRecordBatchReader(&rb_reader)) {
        std::cout << "error" << std::endl;
        return -1;
    }

    /* ---------- total reader to read rows ----------- */
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(FLAGS_path_to_file));

    // Open Parquet file reader
    std::unique_ptr<::parquet::arrow::FileReader> arrow_reader_1;
    ARROW_RETURN_NOT_OK(::parquet::arrow::OpenFile(input, pool, &arrow_reader_1));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader_1->ReadTable(&table));
    int64_t total_rows = table->num_rows();

    /** -----------------writer----------------**/
    auto gzip_codec_options = std::make_shared<::arrow::util::GZipCodecOptions>();
    gzip_codec_options->window_bits = 12;
    if (FLAGS_gzip_format.compare("zlib") == 0) {
      gzip_codec_options->gzip_format = ::arrow::util::GZipFormat::ZLIB;
    } else {
      gzip_codec_options->gzip_format = ::arrow::util::GZipFormat::GZIP;
    }
    std::shared_ptr<::parquet::WriterProperties> writerProperties;
    writerProperties = ::parquet::WriterProperties::Builder()
      .compression(::parquet::Compression::GZIP)
      ->codec_options(gzip_codec_options)
      ->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<::parquet::ArrowWriterProperties> arrow_props =
        ::parquet::ArrowWriterProperties::Builder().store_schema()->build();

    // Create a writer
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(FLAGS_output_path));
    std::unique_ptr<::parquet::arrow::FileWriter> writer = nullptr;
    // ::parquet::arrow::FileWriter::Open(*rb_reader->schema().get(),
    //                                             pool, outfile,
    //                                             writerProperties, arrow_props, &writer);
    // Write each batch as a row_group
    std::shared_ptr<::arrow::RecordBatch> actual_batch;
    for (int i = 0; i < total_rows / batch_size; ++i) {
        if (arrow::Status::OK() != rb_reader->ReadNext(&actual_batch)) {
            std::cout << "error " << std::endl;
            return -1;
        }
        if (i == 0) {
            auto status = ::parquet::arrow::FileWriter::Open(*actual_batch->schema(),
                    pool, outfile,
                    writerProperties, arrow_props, &writer);
            if (arrow::Status::OK() != status) {
                std::cout << "error when open file: " << std::endl;
            }
        }
        ARROW_ASSIGN_OR_RAISE(auto writerTable,
                          arrow::Table::FromRecordBatches(actual_batch->schema(), {actual_batch}));
        // writer->WriteTable(*writerTable.get(), actual_batch->num_rows());
        ARROW_RETURN_NOT_OK(writer->WriteTable(*writerTable.get(), actual_batch->num_rows()));
    }
    if (table->num_rows() % batch_size > 0) {
        if (arrow::Status::OK() != rb_reader->ReadNext(&actual_batch)) {
            std::cout << "error " << std::endl;
            return -1;
        }
        if (writer == nullptr) {
            auto status = ::parquet::arrow::FileWriter::Open(*actual_batch->schema(),
                    pool, outfile,
                    writerProperties, arrow_props, &writer);
            if (arrow::Status::OK() != status) {
                std::cout << "error when open file: " << std::endl;
            }
        }
        ARROW_ASSIGN_OR_RAISE(auto writerTable,
                          arrow::Table::FromRecordBatches(actual_batch->schema(), {actual_batch}));
        ARROW_RETURN_NOT_OK(writer->WriteTable(*writerTable.get(), actual_batch->num_rows()));
    }

    // Write file footer and close
    ARROW_RETURN_NOT_OK(writer->Close());
    return 0;
  }


 private:
  const bool disableDictionary_;
};

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary) {
  ParquetTransferBenchmark benchmark(disableDictionary);
  BIGINT()->toString();
  (void)benchmark.writeToFile1();
}

#define PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      false);                                                             \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 100)

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 0)   \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_NO_FILTER(_type_, _name_) \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100)    \
  BENCHMARK_DRAW_LINE();

PARQUET_BENCHMARKS(BIGINT(), BigInt);

// TODO: Add all data types

int main(int argc, char** argv) {
    // sleep(8);
  FLAGS_path_to_file=argv[1];
  FLAGS_output_path=argv[2];
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

