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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

const uint32_t kNumRowsPerBatch = 60000;
const uint32_t kNumBatches = 50;
const uint32_t kNumRowsPerRowGroup = 10000;
const double kFilterErrorMargin = 0.2;

class ParquetReaderBenchmark {
 public:
  explicit ParquetReaderBenchmark(bool disableDictionary)
      : disableDictionary_(disableDictionary) {
    pool_ = memory::addDefaultLeafMemoryPool();
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*pool_.get(), 0);
    auto sink =
        std::make_unique<LocalFileSink>(fileFolder_->path + "/" + fileName_);
    auto sink1 =
        std::make_unique<LocalFileSink>(fileFolder_->path + "/" + fileName1_);        
    std::shared_ptr<::parquet::WriterProperties> writerProperties;
    std::shared_ptr<::parquet::WriterProperties> writerProperties1;


    auto gzip_codec_options = std::make_shared<::arrow::util::GZipCodecOptions>();
    gzip_codec_options->window_bits = 12;
    // gzip_codec_options->compression_level = 9;
    // gzip_codec_options->gzip_format = ::arrow::util::GZipFormat::DEFLATE;
    // gzip_codec_options->gzip_format = ::arrow::util::GZipFormat::GZIP;
    gzip_codec_options->gzip_format = ::arrow::util::GZipFormat::ZLIB;    
    if (disableDictionary_) {
      // The parquet file is in plain encoding format.
      writerProperties1 =
        ::parquet::WriterProperties::Builder().disable_dictionary()->build();
        writerProperties = ::parquet::WriterProperties::Builder()
            .disable_dictionary()
          ->compression(::parquet::Compression::GZIP)
          ->codec_options(gzip_codec_options)
          ->build();          
    } else {
      // The parquet file is in dictionary encoding format.
      writerProperties1 = ::parquet::WriterProperties::Builder().build();
        writerProperties = ::parquet::WriterProperties::Builder()
          .compression(::parquet::Compression::GZIP)
          ->codec_options(gzip_codec_options)
          ->build();        
    }
    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), *pool_, 10000, writerProperties);
    writer1_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink1), *pool_, 10000, writerProperties1);        
  }

  ~ParquetReaderBenchmark() {
    writer_->close();
    writer1_->close();
  }

  void writeToFile(
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) {
    for (auto& batch : batches) {
      writer_->write(batch);
      writer1_->write(batch);
    }
    writer_->flush();
    writer1_->flush();
  }

  FilterSpec createFilterSpec(
      const std::string& columnName,
      float startPct,
      float selectPct,
      const TypePtr& type,
      bool isForRowGroupSkip,
      bool allowNulls) {
    switch (type->childAt(0)->kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kBigintRange,
            isForRowGroupSkip,
            allowNulls);
      case TypeKind::DOUBLE:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kDoubleRange,
            isForRowGroupSkip,
            allowNulls);
      default:
        VELOX_FAIL("Unsupported Data Type {}", type->childAt(0)->toString());
    }
    return FilterSpec(columnName, startPct, selectPct, FilterKind(), false);
  }

  std::shared_ptr<ScanSpec> createScanSpec(
      const std::vector<RowVectorPtr>& batches,
      RowTypePtr& rowType,
      const std::vector<FilterSpec>& filterSpecs,
      std::vector<uint64_t>& hitRows) {
    std::unique_ptr<FilterGenerator> filterGenerator =
        std::make_unique<FilterGenerator>(rowType, 0);
    auto filters = filterGenerator->makeSubfieldFilters(
        filterSpecs, batches, nullptr, hitRows);
    auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const ParquetReaderType& parquetReaderType,
      std::shared_ptr<ScanSpec> scanSpec,
      const RowTypePtr& rowType) {
    dwio::common::ReaderOptions readerOpts{pool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(fileFolder_->path + "/" + fileName_),
        readerOpts.getMemoryPool());
    auto input1 = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(fileFolder_->path + "/" + fileName1_),
        readerOpts.getMemoryPool());

    std::unique_ptr<Reader> reader;
    switch (parquetReaderType) {
      case ParquetReaderType::NATIVE:
        reader = std::make_unique<ParquetReader>(std::move(input), readerOpts);
        break;
      case ParquetReaderType::DUCKDB:
        reader = std::make_unique<ParquetReader>(std::move(input1), readerOpts);
        // reader = std::make_unique<duckdb_reader::ParquetReader>(
        //     input->getInputStream(), readerOpts);
        break;
      default:
        VELOX_UNSUPPORTED("Only native or DuckDB Parquet reader is supported");
    }

    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    return rowReader;
  }

  int read(
      const ParquetReaderType& parquetReaderType,
      const RowTypePtr& rowType,
      std::shared_ptr<ScanSpec> scanSpec,
      uint32_t nextSize) {
    auto rowReader = createReader(parquetReaderType, scanSpec, rowType);

    auto rowReader_duckdb =  createReader(ParquetReaderType::DUCKDB, scanSpec, rowType);
    runtimeStats_ = dwio::common::RuntimeStatistics();

    rowReader->resetFilterCaches();
    rowReader_duckdb->resetFilterCaches();
    auto result = BaseVector::create(rowType, 1, pool_.get());
    auto result_duckdb = BaseVector::create(rowType, 1, pool_.get());
    int resultSize = 0;
    while (true) {
      bool hasData = rowReader->next(nextSize, result);
      bool hasData_duckdb = rowReader_duckdb->next(nextSize, result_duckdb);

      if (!hasData) {
        break;
      }
      if (result->size() == 0) {
        continue;
      }

      auto rowVector = result->as<RowVector>();
      auto rowVector_duckdb = result->as<RowVector>();

      // auto rowVector = result->asUnchecked<RowVector>();
      // auto rowVector_duckdb = result_duckdb->asUnchecked<RowVector>();
      // for (auto i = 0; i < rowVector->childrenSize(); ++i) {
      //   rowVector->childAt(i)->loadedVector();
      //   rowVector_duckdb->childAt(i)->loadedVector();
      // }

      VELOX_CHECK_EQ(
          rowVector->childrenSize(),
          1,
          "The benchmark is performed on single columns. So the result should only contain one column.")

      for (int i = 0; i < rowVector->size(); i++) {
        resultSize += !rowVector->childAt(0)->isNullAt(i);
      }
      auto simpleVector = rowVector->childAt(0)->as<SimpleVector<int32_t>>();
      auto simpleVector_duckdb = rowVector_duckdb->childAt(0)->as<SimpleVector<int32_t>>();
      size_t currentOffset = 0;
      // std::cout << simpleVector->size() << ", resultSize: " << resultSize << std::endl;
      for (auto i = 0; i < simpleVector->size(); i++) {
          if (simpleVector->valueAt(i) != simpleVector_duckdb->valueAt(i)) {
            std::cout << "value not equal" << (int)simpleVector->valueAt(i) << ", right: " << (int)simpleVector_duckdb->valueAt(i) << std::endl;
          }
      }      
    }

    rowReader->updateRuntimeStats(runtimeStats_);
    return resultSize;
  }

  void readSingleColumn(
      const ParquetReaderType& parquetReaderType,
      const std::string& columnName,
      const TypePtr& type,
      float startPct,
      float selectPct,
      uint8_t nullsRateX100,
      uint32_t nextSize) {
    folly::BenchmarkSuspender suspender;

    auto rowType = ROW({columnName}, {type});
    auto batches =
        dataSetBuilder_->makeDataset(rowType, kNumBatches, kNumRowsPerBatch)
            .withRowGroupSpecificData(kNumRowsPerRowGroup)
            .withNullsForField(Subfield(columnName), nullsRateX100)
            .build();
    writeToFile(*batches, true);
    std::vector<FilterSpec> filterSpecs;

    //    Filters on List and Map are not supported currently.
    if (type->kind() != TypeKind::ARRAY && type->kind() != TypeKind::MAP) {
      filterSpecs.emplace_back(createFilterSpec(
          columnName, startPct, selectPct, rowType, false, false));
    }

    std::vector<uint64_t> hitRows;
    auto scanSpec = createScanSpec(*batches, rowType, filterSpecs, hitRows);

    suspender.dismiss();

    // Filter range is generated from a small sample data of 4096 rows. So the
    // upperBound and lowerBound are introduced to estimate the result size.
    auto resultSize = read(parquetReaderType, rowType, scanSpec, nextSize);
    std::cout << "resultSize: " << resultSize << std::endl;

    // Add one to expected to avoid 0 in calculating upperBound and lowerBound.
    int expected = kNumBatches * kNumRowsPerBatch *
            (1 - (double)nullsRateX100 / 100) * ((double)selectPct / 100) +
        1;

    // Make the upperBound and lowerBound large enough to avoid very small
    // resultSize and expected size, where the diff ratio is relatively very
    // large.
    int upperBound = expected * (1 + kFilterErrorMargin) + 1;
    int lowerBound = expected * (1 - kFilterErrorMargin) - 1;
    upperBound = std::max(16, upperBound);
    lowerBound = std::max(0, lowerBound);

    VELOX_CHECK(
        resultSize <= upperBound && resultSize >= lowerBound,
        "Result Size {} and Expected Size {} Mismatch",
        resultSize,
        expected);
  }

 private:
  const std::string fileName_ = "test.parquet";
  const std::string fileName1_ = "1_test.parquet";
  const std::shared_ptr<facebook::velox::exec::test::TempDirectoryPath>
      fileFolder_ = facebook::velox::exec::test::TempDirectoryPath::create();
  const bool disableDictionary_;

  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> pool_;
  dwio::common::DataSink* sinkPtr_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer1_;
  RuntimeStatistics runtimeStats_;
};

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary) {
  ParquetReaderBenchmark benchmark(disableDictionary);
  BIGINT()->toString();
  benchmark.readSingleColumn(
      ParquetReaderType::NATIVE,
      columnName,
      type,
      0,
      filterRateX100,
      nullsRateX100,
      nextSize);
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
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_plain,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_Plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_dict,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_plain,      \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      true);                                                              \
  BENCHMARK_DRAW_LINE();


#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 0)  

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100) \
  BENCHMARK_DRAW_LINE();


// PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(INTEGER(), INTEGER);
// PARQUET_BENCHMARKS(DOUBLE(), Double);
// PARQUET_BENCHMARKS_NO_FILTER(MAP(BIGINT(), BIGINT()), Map);
// PARQUET_BENCHMARKS_NO_FILTER(ARRAY(BIGINT()), List);

// TODO: Add all data types

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}


