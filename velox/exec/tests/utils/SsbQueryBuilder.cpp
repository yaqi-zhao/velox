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
        // for (int i = 0; i < types.size(); i++) {
        //   std::cout << "i: " << i << ", type: " << types[i]->kind() << std::endl;
        // }
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
    case 2:
      return getQ2Plan();
    case 3:
      return getQ3Plan();
    case 4:
      return getQ4Plan();
    case 5:
      return getQ5Plan();
    case 6:
      return getQ6Plan();
    case 7:
      return getQ7Plan();      
    case 8:
      return getQ8Plan(); 
    case 9:
      return getQ9Plan(); 
    case 10:
      return getQ10Plan(); 
    case 11:
      return getQ11Plan(); 
    case 12:
      return getQ12Plan(); 
    case 13:
      return getQ13Plan();
    case 30:
      return getQ30Plan();                                      
    case 31:
      return getQ31Plan();                
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

SsbPlan SsbQueryBuilder::getQ31Plan() const {
  std::vector<std::string> selectedColumns = {"lo_extendedprice",
      "lo_discount",
      "lo_orderdate",
      "lo_quantity"};

  const auto selectedRowType = getRowType(kLineorderFlat, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineorderFlat);

  auto quantityFilter = "lo_extendedprice > 1";

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(
                      kLineorderFlat,
                      selectedRowType,
                      fileColumnNames,
                      {quantityFilter})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"lo_extendedprice"})
                  .partialAggregation({}, {"count(lo_extendedprice)"})
                  .finalAggregation()
                  .localPartition({})
                  .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

SsbPlan SsbQueryBuilder::getQ30Plan() const {
  std::vector<std::string> selectedColumns = {
      "lo_orderdate"};
      // "lo_orderkey",
      // "lo_discount"};

  const auto selectedRowType = getRowType(kLineorderFlat, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineorderFlat);
  

  auto orderDateFilter = formatDateFilter("lo_orderdate", selectedRowType, "'1993-10-01'", "'1993-12-31'");

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(
                      kLineorderFlat,
                      selectedRowType,
                      fileColumnNames,
                      {"lo_orderdate > '1993-01-01'::DATE"})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"lo_orderdate"})
                  // .filter("(lo_orderkey> 300)")
                  .partialAggregation({}, {"count(lo_orderdate)"})
                  .finalAggregation()
                  .localPartition({})
                  .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

// Q1.1 SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
// FROM lineorder_flat
// WHERE toYear(LO_ORDERDATE) = 1993 AND LO_DISCOUNT BETWEEN 1 AND 3 AND LO_QUANTITY < 25;
SsbPlan SsbQueryBuilder::getQ1Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderkey",
      "lo_orderdate",
      "lo_extendedprice",
      "lo_discount",
      "p_brand"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);
  auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1993-10-01'", "'1993-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {"lo_orderkey > 300"})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_extendedprice * lo_discount AS part_revenue", "lo_orderdate", "lo_discount", "lo_orderkey"})
                    .filter("(lo_discount between 1 and 3)  AND (lo_orderdate > '1993-01-01'::DATE) AND (lo_orderdate < '1993-12-31'::DATE)")
                    .partialAggregation({},{"sum(part_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

// Q1.2: SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
// FROM lineorder_flat
// WHERE toYYYYMM(LO_ORDERDATE) = 199401 AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;
SsbPlan SsbQueryBuilder::getQ2Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderdate",
      "lo_extendedprice",
      "lo_discount",
      "lo_quantity"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);
  auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1994-04-01'", "'1994-04-30'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_extendedprice * lo_discount AS part_revenue", "lo_discount", "lo_orderdate", "lo_quantity"})
                    .filter("(lo_discount between 4 and 6) AND (lo_quantity between 26 and 35) AND (lo_orderdate > '1994-04-01'::DATE) AND (lo_orderdate < '1994-04-30'::DATE)")
                    .partialAggregation({},{"sum(part_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q1.3
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM lineorder_flat
WHERE toISOWeek(LO_ORDERDATE) = 6 AND toYear(LO_ORDERDATE) = 1994
  AND LO_DISCOUNT BETWEEN 5 AND 7 AND LO_QUANTITY BETWEEN 26 AND 35;
*/
SsbPlan SsbQueryBuilder::getQ3Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderdate",
      "lo_extendedprice",
      "lo_discount",
      "lo_quantity"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);
  auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1994-01-01'", "'1994-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_extendedprice * lo_discount AS part_revenue", "lo_orderdate", "lo_quantity" ,"lo_discount"})
                    .filter("(lo_discount between 5 and 7) AND (lo_quantity between 26 and 35) AND (lo_orderdate > '1994-01-01'::DATE) AND (lo_orderdate < '1994-12-30'::DATE)")
                    .partialAggregation({},{"sum(part_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q2.1
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
*/
SsbPlan SsbQueryBuilder::getQ4Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderdate",
      "lo_revenue",
      "p_category",
      "p_brand",
      "s_region"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    // .filter("(p_category = 'MFGR#12'::VARBINARY) AND (S_REGION = 'AMERICA'::VARBINARY)")
                    .project({"lo_revenue", "p_brand", "p_category", "s_region"})
                    .filter("(p_category = 12) AND (s_region = 0)")
                    .partialAggregation({"p_brand"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"p_brand"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/*  Q2.2
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE P_BRAND >= 'MFGR#2221' AND P_BRAND <= 'MFGR#2228' AND S_REGION = 'ASIA'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
*/
SsbPlan SsbQueryBuilder::getQ5Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "p_category",
      "p_brand",
      "s_region"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    // .filter("(p_brand >= 'MFGR#2221'::VARBINARY) AND (p_brand <= 'MFGR#2228'::VARBINARY) AND (s_region = 'ASIA'::VARBINARY)")
                    .filter("(p_brand >= 'Brand#40') AND (p_brand <= 'Brand#45') AND (s_region = 1)")
                    .project({"lo_revenue", "lo_orderyear", "p_brand", "s_region"})
                    .partialAggregation({"lo_orderyear", "p_brand"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "p_brand"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q2.3
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
    */
SsbPlan SsbQueryBuilder::getQ6Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "p_category",
      "p_brand",
      "s_region"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .filter("(p_brand = 'Brand#55') AND (s_region = 2)")
                    .project({"lo_revenue", "lo_orderyear", "p_brand", "s_region"})
                    .partialAggregation({"lo_orderyear", "p_brand"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "p_brand"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q3.1
SELECT
    C_NATION,
    S_NATION,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_REGION = 'ASIA' AND S_REGION = 'ASIA' AND year >= 1992 AND year <= 1997
GROUP BY
    C_NATION,
    S_NATION,
    year
ORDER BY
    year ASC,
    revenue DESC;
*/
SsbPlan SsbQueryBuilder::getQ7Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "s_region",
      "c_region",
      "c_nation",
      "s_nation"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  // auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1992-01-01'", "'1997-12-31'");

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_revenue", "lo_orderyear", "c_nation", "s_nation", "c_region", "s_region"})
                    .filter("(c_region = 1) AND (s_region = 1) AND (lo_orderyear between 1992 and 1997)")
                    .partialAggregation({"c_nation", "s_nation", "lo_orderyear"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "revenue"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q3.2
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_NATION = 'UNITED STATES' AND S_NATION = 'UNITED STATES' AND year >= 1992 AND year <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
*/
SsbPlan SsbQueryBuilder::getQ8Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "c_city",
      "s_city",
      "c_nation",
      "s_nation"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  // auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1992-01-01'", "'1997-12-31'");

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_revenue", "lo_orderyear", "c_city", "s_city", "s_nation", "c_nation"})
                    .filter("(c_nation = 3) AND (s_nation = 3) AND (lo_orderyear between 1992 and 1997)")
                    .partialAggregation({"c_city", "s_city", "lo_orderyear"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "revenue"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q3.3
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5') AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5') AND year >= 1992 AND year <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
*/
SsbPlan SsbQueryBuilder::getQ9Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "c_city",
      "s_city",
      "c_nation",
      "s_nation"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  // auto orderDateFilter = formatDateFilter("lo_orderdate", lineorderSelectedRowType, "'1992-01-01'", "'1997-12-31'");

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_revenue", "lo_orderyear", "c_city", "s_city"})
                    .filter("((c_city = 111) OR (c_city = 110)) AND ((s_city = 111) OR (s_city = 110)) AND (lo_orderyear between 1992 and 1997)")
                    .partialAggregation({"c_city", "s_city", "lo_orderyear"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "revenue"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q3.4
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5') AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5') AND toYYYYMM(LO_ORDERDATE) = 199712
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
*/
SsbPlan SsbQueryBuilder::getQ10Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_orderdate",
      "lo_revenue",
      "c_city",
      "s_city",
      "c_nation",
      "s_nation"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  // auto orderDateFilter = formatDateFilter("lo_orderyear", lineorderSelectedRowType, "'1997-12-01'", "'1997-12-31'");
  // std::cout << "orderDateFilter: " << orderDateFilter << std::endl;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"lo_revenue", "lo_orderyear", "lo_orderdate", "c_city", "s_city"})
                    .filter("(c_city = 111 OR c_city = 110) AND (s_city = 111 OR s_city = 110) AND (lo_orderdate > '1997-12-01'::DATE) AND (lo_orderdate < '1997-12-31'::DATE)")
                    .partialAggregation({"c_city", "s_city", "lo_orderyear"},{"sum(lo_revenue) as revenue"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "revenue"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q4.1
SELECT
    toYear(LO_ORDERDATE) AS year,
    C_NATION,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    year,
    C_NATION
ORDER BY
    year ASC,
    C_NATION ASC;
*/
SsbPlan SsbQueryBuilder::getQ11Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "c_nation",
      "c_region",
      "s_region",
      "p_mfgr",
      "lo_supplycost"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;


  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .filter("(c_region = 3) AND (s_region = 3) AND (p_mfgr = 'Manufacturer#1' OR p_mfgr = 'Manufacturer#2')")
                    .project({"lo_orderyear", "(lo_revenue - lo_supplycost) as profit", "c_nation"})
                    .partialAggregation({"lo_orderyear", "c_nation"},{"sum(profit) as l_profit"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "c_nation"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q4.2
SELECT
    toYear(LO_ORDERDATE) AS year,
    S_NATION,
    P_CATEGORY,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (year = 1997 OR year = 1998) AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    year,
    S_NATION,
    P_CATEGORY
ORDER BY
    year ASC,
    S_NATION ASC,
    P_CATEGORY ASC;
*/
SsbPlan SsbQueryBuilder::getQ12Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "s_nation",
      "c_region",
      "s_region",
      "p_mfgr",
      "lo_supplycost",
      "p_category"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"c_region", "s_region", "p_mfgr", "lo_orderyear", "s_nation", "p_category", "(lo_revenue - lo_supplycost) as l_profit"})
                    .filter("(c_region = 2) AND (s_region = 2) AND (p_mfgr = 'Manufacturer#1' OR p_mfgr = 'Manufacturer#2') AND (lo_orderyear = 1997 OR lo_orderyear = 1998)")
                    .partialAggregation({"lo_orderyear", "s_nation", "p_category"},{"sum(l_profit) as profit"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "s_nation", "p_category"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

/* Q4.3
SELECT
    toYear(LO_ORDERDATE) AS year,
    S_CITY,
    P_BRAND,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE S_NATION = 'UNITED STATES' AND (year = 1997 OR year = 1998) AND P_CATEGORY = 'MFGR#14'
GROUP BY
    year,
    S_CITY,
    P_BRAND
ORDER BY
    year ASC,
    S_CITY ASC,
    P_BRAND ASC;
*/
SsbPlan SsbQueryBuilder::getQ13Plan() const {
  std::vector<std::string> lineorderColumns = {
      "lo_orderyear",
      "lo_revenue",
      "s_nation",
      "s_city",
      "p_brand",
      "lo_supplycost",
      "p_category"
      };

  const auto lineorderSelectedRowType = getRowType(kLineorderFlat, lineorderColumns);
  const auto& lineorderFileColumns = getFileColumnNames(kLineorderFlat);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineorderPlanNodeId;

  parse::ParseOptions options = {false, false};

  auto plan = PlanBuilder(planNodeIdGenerator).setParseOptions(options)
                    .tableScan(
                      kLineorderFlat,
                      lineorderSelectedRowType,
                      lineorderFileColumns,
                      {})
                    .capturePlanNodeId(lineorderPlanNodeId)
                    .project({"p_category", "lo_orderyear", "s_city", "p_brand", "s_nation", "(lo_revenue - lo_supplycost) as l_profit"})
                    .filter("(s_nation = 18) AND (lo_orderyear = 1997 OR lo_orderyear = 1998) AND (p_category = 14)")
                    .partialAggregation({"lo_orderyear", "s_city", "p_brand"},{"sum(l_profit) as profit"})
                    .localPartition({})
                    .finalAggregation()
                    .orderBy({"lo_orderyear", "s_city", "p_brand"}, false)
                    .planNode();

  SsbPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineorderPlanNodeId] = getTableFilePaths(kLineorderFlat);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> SsbQueryBuilder::kTableNames_ = {
    kLineorderFlat};

const std::unordered_map<std::string, std::vector<std::string>>
    SsbQueryBuilder::kTables_ = {
        std::make_pair(
            "lineorder_flat",
            tpch::getTableSchema(tpch::Table::TBL_LINEORDER_FLAT)->names())};

} // namespace facebook::velox::exec::test
