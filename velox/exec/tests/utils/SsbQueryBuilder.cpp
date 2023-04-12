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
#include "velox/exec/tests/utils/SsbQueryBuilder.h"

#include "velox/common/base/Fs.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::exec::test {

namespace {
int64_t toDate(std::string_view stringDate) {
  Date date;
  parseTo(stringDate, date);
  return date.days();
}

/// DWRF does not support Date type and Varchar is used.
/// Return the Date filter expression as per data format.
std::string formatDateFilter(
    const std::string& stringDate,
    const RowTypePtr& rowType,
    const std::string& lowerBound,
    const std::string& upperBound) {
  bool isDwrf = rowType->findChild(stringDate)->isVarchar();
  auto suffix = isDwrf ? "" : "::DATE";

  if (!lowerBound.empty() && !upperBound.empty()) {
    return fmt::format(
        "{} between {}{} and {}{}",
        stringDate,
        lowerBound,
        suffix,
        upperBound,
        suffix);
  } else if (!lowerBound.empty()) {
    return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
  } else if (!upperBound.empty()) {
    return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
  }

  VELOX_FAIL(
      "Date range check expression must have either a lower or an upper bound");
}

std::vector<std::string> mergeColumnNames(
    const std::vector<std::string>& firstColumnVector,
    const std::vector<std::string>& secondColumnVector) {
  std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
  mergedColumnVector.insert(
      mergedColumnVector.end(),
      secondColumnVector.begin(),
      secondColumnVector.end());
  return mergedColumnVector;
};
} // namespace

void SsbQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        dwio::common::ReaderOptions readerOptions{pool_.get()};
        readerOptions.setFileFormat(format_);
        auto input = std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(dirEntry.path().string()),
            readerOptions.getMemoryPool());
        std::unique_ptr<dwio::common::Reader> reader =
            dwio::common::getReaderFactory(readerOptions.getFileFormat())
                ->createReader(std::move(input), readerOptions);
        const auto fileType = reader->rowType();
        const auto fileColumnNames = fileType->names();
        // There can be extra columns in the file towards the end.
        VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
        std::unordered_map<std::string, std::string> fileColumnNamesMap(
            columns.size());
        std::transform(
            columns.begin(),
            columns.end(),
            fileColumnNames.begin(),
            std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
            [](std::string a, std::string b) { return std::make_pair(a, b); });
        auto columnNames = columns;
        auto types = fileType->children();
        types.resize(columnNames.size());
        tableMetadata_[tableName].type =
            std::make_shared<RowType>(std::move(columnNames), std::move(types));
        tableMetadata_[tableName].fileColumnNames =
            std::move(fileColumnNamesMap);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
  }
}

const std::vector<std::string>& SsbQueryBuilder::getTableNames() {
  return kTableNames_;
}

SsbPlan SsbQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getQ1Plan();
    // case 3:
    //   return getQ3Plan();
    // case 5:
    //   return getQ5Plan();
    case 31:
      return getQ31Plan();                
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

SsbPlan SsbQueryBuilder::getQ1Plan() const {
  std::vector<std::string> selectedColumns = {"lo_extendedprice"};

  const auto selectedRowType = getRowType(kLineorderFlat, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineorderFlat);

  auto quantityFilter = "lo_extendedprice > 1";

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(
                      kLineorderFlat,
                      selectedRowType,
                      fileColumnNames,
                      {})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"lo_extendedprice"})
                  .partialAggregation({}, {"count(lo_extendedprice)"})
                  .finalAggregation()
                  .localPartition({})
                  .planNode();

//   auto plan = PlanBuilder()
//                   .tableScan(
//                       kTest,
//                       selectedRowType,
//                       fileColumnNames,
//                       {})
//                   .capturePlanNodeId(lineitemPlanNodeId)
//                   .project({"column_1"})
//                   .localPartition({})
//                   .filter("column_1 > 30")
//                   .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

SsbPlan SsbQueryBuilder::getQ31Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_extendedprice",
      "lo_discount",
      "lo_orderdate",
      "lo_quantity"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);
  auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1993-10-01'", "'1993-12-31'");


  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;


  auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {orderDateFilter})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .filter("(lo_discount between 1 and 3) AND (lo_quantity < 25) ")
                    .project({"lo_extendedprice * lo_discount AS part_revenue"})
                    .partialAggregation({},{"sum(part_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .project({"revenue"})
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}


const std::vector<std::string> SsbQueryBuilder::kTableNames_ = {
    kLineorderFlat2,
    kLineorderFlat};

const std::unordered_map<std::string, std::vector<std::string>>
    SsbQueryBuilder::kTables_ = {
        std::make_pair(
            "lineorder_flat",
            tpch::getTableSchema(tpch::Table::TBL_LINEORDER_FLAT)->names()),                                            
        std::make_pair(
            "lineorder_flat_2",
            tpch::getTableSchema(tpch::Table::TBL_LINEORDER_FLAT_2)->names())};

} // namespace facebook::velox::exec::test
