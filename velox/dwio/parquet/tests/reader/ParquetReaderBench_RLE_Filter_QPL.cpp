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
#include "velox/dwio/parquet/qpl_reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/dwio/common/QplJobPool.h"

using std::chrono::system_clock;

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
    pool_ = memory::getDefaultMemoryPool();
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*pool_.get(), 0);
  }

  ~ParquetReaderBenchmark() {
    // writer_->close();
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
      case TypeKind::SMALLINT:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kBigintRange,
            isForRowGroupSkip,
            false);
      case TypeKind::DOUBLE:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kDoubleRange,
            isForRowGroupSkip,
            allowNulls);
      case TypeKind::REAL:
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
    auto filters =
        filterGenerator->makeSubfieldFilters(filterSpecs, batches, hitRows);
    auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const ParquetReaderType& parquetReaderType,
      std::shared_ptr<ScanSpec> scanSpec,
      const RowTypePtr& rowType,
      uint32_t nextSize) {
    dwio::common::ReaderOptions readerOpts{pool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>("/tmp/test_uint_" + std::to_string(nextSize) + ".parquet"),
        readerOpts.getMemoryPool());

    std::unique_ptr<Reader> reader;
    switch (parquetReaderType) {
      case ParquetReaderType::NATIVE:
        reader = std::make_unique<ParquetReader>(std::move(input), readerOpts);
        break;
      case ParquetReaderType::DUCKDB:
        reader = std::make_unique<duckdb_reader::ParquetReader>(
            input->getInputStream(), readerOpts);
        break;
      case ParquetReaderType::QPL:
        reader = std::make_unique<qpl_reader::ParquetReader>(std::move(input), readerOpts);
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
    auto rowReader = createReader(parquetReaderType, scanSpec, rowType, nextSize);
    runtimeStats_ = dwio::common::RuntimeStatistics();

    rowReader->resetFilterCaches();
    auto result = BaseVector::create(rowType, 1, pool_.get());
    int resultSize = 0;
    // auto startTime = system_clock::now();
    while (true) {
      bool hasData = rowReader->next(nextSize, result);

      if (!hasData) {
        break;
      }
      if (result->size() == 0) {
        continue;
      }

      auto rowVector = result->asUnchecked<RowVector>();
      for (auto i = 0; i < rowVector->childrenSize(); ++i) {
        rowVector->childAt(i)->loadedVector();
      }

      VELOX_CHECK_EQ(
          rowVector->childrenSize(),
          1,
          "The benchmark is performed on single columns. So the result should only contain one column.")

      for (int i = 0; i < rowVector->size(); i++) {
        resultSize += !rowVector->childAt(0)->isNullAt(i);
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
            .withRowGroupSpecificData(nextSize)
            .withNullsForField(Subfield(columnName), nullsRateX100)
            .build();
    std::vector<FilterSpec> filterSpecs;

    //    Filters on List and Map are not supported currently.
    if (type->kind() != TypeKind::ARRAY && type->kind() != TypeKind::MAP) {
      filterSpecs.emplace_back(createFilterSpec(
          columnName, startPct, selectPct, rowType, false, false));
    }

    std::vector<uint64_t> hitRows;
    auto scanSpec = createScanSpec(*batches, rowType, filterSpecs, hitRows);
    // auto scanSpec = nullptr;

    suspender.dismiss();
    // auto startTime = system_clock::now();

    // Filter range is generated from a small sample data of 4096 rows. So the
    // upperBound and lowerBound are introduced to estimate the result size.
    auto resultSize = read(parquetReaderType, rowType, scanSpec, nextSize);

    // auto curTime = system_clock::now();
    // size_t msElapsed = std::chrono::duration_cast<std::chrono::microseconds>(
    //       curTime - startTime).count();
    
    // printf("ParquetReader_%d_%.0f_%.0f    time:%dus\n", int(nextSize), startPct, selectPct, (int)(msElapsed));        
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

    // VELOX_CHECK(
    //     resultSize <= upperBound && resultSize >= lowerBound,
    //     "Result Size {} and Expected Size {} Mismatch",
    //     resultSize,
    //     expected);
  }

 private:
  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> pool_;
  dwio::common::DataSink* sinkPtr_;
  // std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  RuntimeStatistics runtimeStats_;
  bool disableDictionary_;
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
      ParquetReaderType::QPL,
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
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_60k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      60000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_40k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      40000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_30k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      30000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                               \
      false);                                                             \  
    
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 0)  
  // PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 20) \
  // PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 50) \
  // PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 70) \
  // PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 100)

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 0)   \
  // PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 1)   \
  // PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 20)  \
  // PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 50)  \
  // PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 70)  \
  // PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100) \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_NO_FILTER(_type_, _name_) \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100)    \
  BENCHMARK_DRAW_LINE();



// PARQUET_BENCHMARKS(BOOLEAN(), BOOLEAN);
// PARQUET_BENCHMARKS(TINYINT(), TINYINT);
PARQUET_BENCHMARKS(SMALLINT(), SMALLINT);
// PARQUET_BENCHMARKS(INTEGER(), INTEGER);

// PARQUET_BENCHMARKS(BIGINT(), BigInt);
// PARQUET_BENCHMARKS(DOUBLE(), Double);
// PARQUET_BENCHMARKS_NO_FILTER(MAP(BIGINT(), BIGINT()), Map);
// PARQUET_BENCHMARKS_NO_FILTER(ARRAY(BIGINT()), List);

// TODO: Add all data types

int main(int argc, char** argv) {
  // sleep(10);
#ifdef VELOX_ENABLE_QPL  
  dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
#endif  
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

