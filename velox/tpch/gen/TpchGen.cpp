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

#include "velox/tpch/gen/TpchGen.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dbgen_gunk.hpp"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dss.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dsstypes.h"
#include "velox/tpch/gen/DBGenIterator.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::tpch {

namespace {

// The cardinality of the LINEITEM table is not a strict multiple of SF since
// the number of lineitems in an order is chosen at random with an average of
// four. This function contains the row count for all authorized scale factors
// (as described by the TPC-H spec), and approximates the remaining.
constexpr size_t getLineItemRowCount(double scaleFactor) {
  auto longScaleFactor = static_cast<long>(scaleFactor);
  switch (longScaleFactor) {
    case 1:
      return 6'001'215;
    case 10:
      return 59'986'052;
    case 30:
      return 179'998'372;
    case 100:
      return 600'037'902;
    case 300:
      return 1'799'989'091;
    case 1'000:
      return 5'999'989'709;
    case 3'000:
      return 18'000'048'306;
    case 10'000:
      return 59'999'994'267;
    case 30'000:
      return 179'999'978'268;
    case 100'000:
      return 599'999'969'200;
    default:
      break;
  }
  return 6'000'000 * scaleFactor;
}

size_t getVectorSize(size_t rowCount, size_t maxRows, size_t offset) {
  if (offset >= rowCount) {
    return 0;
  }
  return std::min(rowCount - offset, maxRows);
}

std::vector<VectorPtr> allocateVectors(
    const RowTypePtr& type,
    size_t vectorSize,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> vectors;
  vectors.reserve(type->size());

  for (const auto& childType : type->children()) {
    vectors.emplace_back(BaseVector::create(childType, vectorSize, pool));
  }
  return vectors;
}

double decimalToDouble(int64_t value) {
  return (double)value * 0.01;
}

Date toDate(std::string_view stringDate) {
  Date date;
  parseTo(stringDate, date);
  return date;
}

} // namespace

std::string_view toTableName(Table table) {
  switch (table) {
    case Table::TBL_PART:
      return "part";
    case Table::TBL_SUPPLIER:
      return "supplier";
    case Table::TBL_PARTSUPP:
      return "partsupp";
    case Table::TBL_CUSTOMER:
      return "customer";
    case Table::TBL_ORDERS:
      return "orders";
    case Table::TBL_LINEITEM:
      return "lineitem";
    case Table::TBL_NATION:
      return "nation";
    case Table::TBL_REGION:
      return "region";
    case Table::TBL_TEST:
      return "test";             
  }
  return ""; // make gcc happy.
}

Table fromTableName(std::string_view tableName) {
  static std::unordered_map<std::string_view, Table> map{
      {"part", Table::TBL_PART},
      {"supplier", Table::TBL_SUPPLIER},
      {"partsupp", Table::TBL_PARTSUPP},
      {"customer", Table::TBL_CUSTOMER},
      {"orders", Table::TBL_ORDERS},
      {"lineitem", Table::TBL_LINEITEM},
      {"nation", Table::TBL_NATION},
      {"region", Table::TBL_REGION},
      {"test", Table::TBL_TEST},
  };

  auto it = map.find(tableName);
  if (it != map.end()) {
    return it->second;
  }
  throw std::invalid_argument(
      fmt::format("Invalid TPC-H table name: '{}'", tableName));
}

size_t getRowCount(Table table, double scaleFactor) {
  VELOX_CHECK_GE(scaleFactor, 0, "Tpch scale factor must be non-negative");
  switch (table) {
    case Table::TBL_PART:
      return 200'000 * scaleFactor;
    case Table::TBL_SUPPLIER:
      return 10'000 * scaleFactor;
    case Table::TBL_PARTSUPP:
      return 800'000 * scaleFactor;
    case Table::TBL_CUSTOMER:
      return 150'000 * scaleFactor;
    case Table::TBL_ORDERS:
      return 1'500'000 * scaleFactor;
    case Table::TBL_NATION:
      return 25;
    case Table::TBL_REGION:
      return 5;
    case Table::TBL_LINEORDER_FLAT:
      return getLineItemRowCount(scaleFactor);      
    case Table::TBL_LINEITEM:
      return getLineItemRowCount(scaleFactor);
  }
  return 0; // make gcc happy.
}

RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case Table::TBL_PART: {
      static RowTypePtr type = ROW(
          {
              "p_partkey",
              "p_name",
              "p_mfgr",
              "p_brand",
              "p_type",
              "p_size",
              "p_container",
              "p_retailprice",
              "p_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_SUPPLIER: {
      static RowTypePtr type = ROW(
          {
              "s_suppkey",
              "s_name",
              "s_address",
              "s_nationkey",
              "s_phone",
              "s_acctbal",
              "s_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_PARTSUPP: {
      static RowTypePtr type = ROW(
          {
              "ps_partkey",
              "ps_suppkey",
              "ps_availqty",
              "ps_supplycost",
              "ps_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              INTEGER(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_CUSTOMER: {
      static RowTypePtr type = ROW(
          {
              "c_custkey",
              "c_name",
              "c_address",
              "c_nationkey",
              "c_phone",
              "c_acctbal",
              "c_mktsegment",
              "c_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_ORDERS: {
      static RowTypePtr type = ROW(
          {
              "o_orderkey",
              "o_custkey",
              "o_orderstatus",
              "o_totalprice",
              "o_orderdate",
              "o_orderpriority",
              "o_clerk",
              "o_shippriority",
              "o_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              DATE(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
          });
      return type;
    }
    case Table::TBL_TEST: {
      static RowTypePtr type = ROW(
          {
            "column_1",
          },
          {
              INTEGER(),
          });
      return type;
    }
    case Table::TBL_TEST_SNAPPY: {
      static RowTypePtr type = ROW(
          {
            "column_1",
          },
          {
              INTEGER(),
          });
      return type;
    }    
    case Table::TBL_LINEORDER_FLAT: {
      static RowTypePtr type = ROW(
          {
            "lo_orderkey",
            "lo_linenumbery",
            "lo_custkey",
            "lo_partkey",
            "lo_suppkey",
            "lo_orderdate",
            "lo_orderpriority",
            "lo_shippriority",
            "lo_quantity",
            "lo_extendedprice",
            "lo_ordtotalprice",
            "lo_discount",
            "lo_revenue",
            "lo_supplycost",
            "lo_tax",
            "lo_commitdate",
            "lo_shipmode",
            "c_name",
            "c_address",
            "c_city",
            "c_nation",
            "c_region",
            "c_phone",
            "c_mktsegment",
            "s_name",
            "s_address",
            "s_city",
            "s_nation",
            "s_region",
            "s_phone",
            "p_name",
            "p_mfgr",
            "p_category",
            "p_brand",
            "p_color",
            "p_type",
            "p_size",
            "p_container",
            "lo_orderyear",
          },
          {
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              DATE(),
              VARCHAR(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              DATE(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              INTEGER(),
          });
      return type;
    }      
    case Table::TBL_LINEORDER_FLAT_2: {
      static RowTypePtr type = ROW(
          {
            "lo_orderkey",
            "lo_custkey",   
          },
          {
              BIGINT(),
              BIGINT(),
          });
      return type;
    }          
    case Table::TBL_LINEITEM: {
      static RowTypePtr type = ROW(
          {
              "l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              INTEGER(),
              DOUBLE(),
              DOUBLE(),
              DOUBLE(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
              DATE(),
              DATE(),
              DATE(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_NATION: {
      static RowTypePtr type = ROW(
          {
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
          });
      return type;
    }
    case Table::TBL_REGION: {
      static RowTypePtr type = ROW(
          {
              "r_regionkey",
              "r_name",
              "r_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }
  }
  return nullptr; // make gcc happy.
}

TypePtr resolveTpchColumn(Table table, const std::string& columnName) {
  return getTableSchema(table)->findChild(columnName);
}

RowVectorPtr genTpchOrders(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto ordersRowType = getTableSchema(Table::TBL_ORDERS);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_ORDERS, scaleFactor), maxRows, offset);
  auto children = allocateVectors(ordersRowType, vectorSize, pool);

  auto orderKeyVector = children[0]->asFlatVector<int64_t>();
  auto custKeyVector = children[1]->asFlatVector<int64_t>();
  auto orderStatusVector = children[2]->asFlatVector<StringView>();
  auto totalPriceVector = children[3]->asFlatVector<double>();
  auto orderDateVector = children[4]->asFlatVector<Date>();
  auto orderPriorityVector = children[5]->asFlatVector<StringView>();
  auto clerkVector = children[6]->asFlatVector<StringView>();
  auto shipPriorityVector = children[7]->asFlatVector<int32_t>();
  auto commentVector = children[8]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initOrder(offset);
  order_t order;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genOrder(i + offset + 1, order);

    orderKeyVector->set(i, order.okey);
    custKeyVector->set(i, order.custkey);
    orderStatusVector->set(i, StringView(&order.orderstatus, 1));
    totalPriceVector->set(i, decimalToDouble(order.totalprice));
    orderDateVector->set(i, toDate(order.odate));
    orderPriorityVector->set(
        i, StringView(order.opriority, strlen(order.opriority)));
    clerkVector->set(i, StringView(order.clerk, strlen(order.clerk)));
    shipPriorityVector->set(i, order.spriority);
    commentVector->set(i, StringView(order.comment, order.clen));
  }
  return std::make_shared<RowVector>(
      pool, ordersRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchLineItem(
    size_t maxOrderRows,
    size_t ordersOffset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // We control the buffer size based on the orders table, then allocate the
  // underlying buffer using the worst case (orderVectorSize * 7).
  size_t orderVectorSize = getVectorSize(
      getRowCount(Table::TBL_LINEITEM, scaleFactor), maxOrderRows, ordersOffset);
  size_t lineItemUpperBound = orderVectorSize * 7;

  // Create schema and allocate vectors.
  auto lineItemRowType = getTableSchema(Table::TBL_LINEITEM);
  auto children = allocateVectors(lineItemRowType, lineItemUpperBound, pool);

  auto orderKeyVector = children[0]->asFlatVector<int64_t>();
  auto partKeyVector = children[1]->asFlatVector<int64_t>();
  auto suppKeyVector = children[2]->asFlatVector<int64_t>();
  auto lineNumberVector = children[3]->asFlatVector<int32_t>();

  auto quantityVector = children[4]->asFlatVector<double>();
  auto extendedPriceVector = children[5]->asFlatVector<double>();
  auto discountVector = children[6]->asFlatVector<double>();
  auto taxVector = children[7]->asFlatVector<double>();

  auto returnFlagVector = children[8]->asFlatVector<StringView>();
  auto lineStatusVector = children[9]->asFlatVector<StringView>();
  auto shipDateVector = children[10]->asFlatVector<Date>();
  auto commitDateVector = children[11]->asFlatVector<Date>();
  auto receiptDateVector = children[12]->asFlatVector<Date>();
  auto shipInstructVector = children[13]->asFlatVector<StringView>();
  auto shipModeVector = children[14]->asFlatVector<StringView>();
  auto commentVector = children[15]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initOrder(ordersOffset);
  order_t order;

  // Dbgen can't generate lineItem one row at a time; instead, it generates
  // orders with a random number of lineitems associated. So we treat offset
  // and maxRows as being in terms of orders (to make it deterministic), and
  // return a RowVector with a variable number of rows.
  size_t lineItemCount = 0;

  for (size_t i = 0; i < orderVectorSize; ++i) {
    dbgenIt.genOrder(i + ordersOffset + 1, order);

    for (size_t l = 0; l < order.lines; ++l) {
      const auto& line = order.l[l];
      orderKeyVector->set(lineItemCount + l, line.okey);
      partKeyVector->set(lineItemCount + l, line.partkey);
      suppKeyVector->set(lineItemCount + l, line.suppkey);

      lineNumberVector->set(lineItemCount + l, line.lcnt);

      quantityVector->set(lineItemCount + l, decimalToDouble(line.quantity));
      extendedPriceVector->set(lineItemCount + l, decimalToDouble(line.eprice));
      discountVector->set(lineItemCount + l, decimalToDouble(line.discount));
      taxVector->set(lineItemCount + l, decimalToDouble(line.tax));

      returnFlagVector->set(lineItemCount + l, StringView(line.rflag, 1));
      lineStatusVector->set(lineItemCount + l, StringView(line.lstatus, 1));

      shipDateVector->set(lineItemCount + l, toDate(line.sdate));
      commitDateVector->set(lineItemCount + l, toDate(line.cdate));
      receiptDateVector->set(lineItemCount + l, toDate(line.rdate));

      shipInstructVector->set(
          lineItemCount + l,
          StringView(line.shipinstruct, strlen(line.shipinstruct)));
      shipModeVector->set(
          lineItemCount + l, StringView(line.shipmode, strlen(line.shipmode)));
      commentVector->set(
          lineItemCount + l, StringView(line.comment, strlen(line.comment)));
    }
    lineItemCount += order.lines;
  }

  // Resize to shrink the buffers - since we allocated based on the upper bound.
  for (auto& child : children) {
    child->resize(lineItemCount);
  }
  return std::make_shared<RowVector>(
      pool,
      lineItemRowType,
      BufferPtr(nullptr),
      lineItemCount,
      std::move(children));
}


RowVectorPtr genTpchLineOrderFlat(
    size_t maxOrderRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // We control the buffer size based on the orders table, then allocate the
  // underlying buffer using the worst case (vectorSize * 7).
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_LINEORDER_FLAT, scaleFactor), maxOrderRows, offset);
  // size_t vectorSize = getRowCount(Table::TBL_LINEORDER_FLAT, scaleFactor);
  // size_t lineItemUpperBound = vectorSize * 7;

  // Create schema and allocate vectors.
  auto lineItemRowType = getTableSchema(Table::TBL_LINEORDER_FLAT);
  auto children = allocateVectors(lineItemRowType, vectorSize, pool);

  auto orderKeyVector = children[0]->asFlatVector<int32_t>();
  auto linenumberVector = children[1]->asFlatVector<int32_t>();
  auto custkeyVector = children[2]->asFlatVector<int32_t>();
  auto partkeyVector = children[3]->asFlatVector<int32_t>();
  auto suppkeyVector = children[4]->asFlatVector<int32_t>();
  auto orderdateVector = children[5]->asFlatVector<Date>();
  auto orderpriorityVector = children[6]->asFlatVector<StringView>();
  auto shippriorityVector = children[7]->asFlatVector<int32_t>();
  auto quantityVector = children[8]->asFlatVector<int32_t>();
  auto extendedpriceVector = children[9]->asFlatVector<int32_t>();
  auto ordtotalpriceVector = children[10]->asFlatVector<int32_t>();
  auto discountVector = children[11]->asFlatVector<int32_t>();
  auto revenueVector = children[12]->asFlatVector<int32_t>();
  auto supplycostVector = children[13]->asFlatVector<int32_t>();
  auto taxVector = children[14]->asFlatVector<int32_t>();
  auto commitdateVector = children[15]->asFlatVector<Date>();

  auto shipmodeVector = children[16]->asFlatVector<StringView>();
  auto cnameVector = children[17]->asFlatVector<StringView>();
  auto caddressVector = children[18]->asFlatVector<StringView>();
  auto ccityVector = children[19]->asFlatVector<int32_t>();
  auto cnationVector = children[20]->asFlatVector<int32_t>();
  auto cregionVector = children[21]->asFlatVector<int32_t>();
  auto cphoneVector = children[22]->asFlatVector<StringView>();
  auto cmktsegmentVector = children[23]->asFlatVector<StringView>();
  auto snameVector = children[24]->asFlatVector<StringView>();
  auto saddressVector = children[25]->asFlatVector<StringView>();
  auto scityVector = children[26]->asFlatVector<int32_t>();
  auto snationVector = children[27]->asFlatVector<int32_t>();
  auto sregionVector = children[28]->asFlatVector<int32_t>();
  auto sphoneVector = children[29]->asFlatVector<StringView>();
  auto pnameVector = children[30]->asFlatVector<StringView>();
  auto pmfgrVector = children[31]->asFlatVector<StringView>();
  auto pcategoryVector = children[32]->asFlatVector<int32_t>();
  auto pbrandVector = children[33]->asFlatVector<StringView>();
  auto pcolorVector = children[34]->asFlatVector<int32_t>();
  auto ptypeVector = children[35]->asFlatVector<StringView>();
  auto psizeVector = children[36]->asFlatVector<int32_t>();
  auto pcontainerVector = children[37]->asFlatVector<StringView>();
  auto orderyearVector = children[38]->asFlatVector<int32_t>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initLineorderFlat(offset);
  lineorder_flat_t lineorder_flat;

  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genLineorderFlat(i + offset + 1, lineorder_flat);

    orderKeyVector->set(i, lineorder_flat.orderKey);
    linenumberVector->set(i, lineorder_flat.linenumber);
    custkeyVector->set(i, lineorder_flat.custkey);
    partkeyVector->set(i, lineorder_flat.partkey);
    suppkeyVector->set(i, lineorder_flat.suppkey);
    orderdateVector->set(i, toDate(lineorder_flat.orderdate));
    orderpriorityVector->set(i, StringView(lineorder_flat.orderpriority, strlen(lineorder_flat.orderpriority)));
    shippriorityVector->set(i, lineorder_flat.shippriority);
    quantityVector->set(i, lineorder_flat.quantity);
    extendedpriceVector->set(i, lineorder_flat.extendedprice);
    ordtotalpriceVector->set(i, lineorder_flat.ordtotalprice);
    discountVector->set(i, lineorder_flat.discount);
    revenueVector->set(i, lineorder_flat.revenue);
    supplycostVector->set(i, lineorder_flat.supplycost);
    taxVector->set(i, lineorder_flat.tax);
    commitdateVector->set(i, toDate(lineorder_flat.commitdate));

    shipmodeVector->set(i, StringView(lineorder_flat.shipmode, strlen(lineorder_flat.shipmode)));
    cnameVector->set(i, StringView(lineorder_flat.cname, strlen(lineorder_flat.cname)));
    caddressVector->set(i, StringView(lineorder_flat.caddress, strlen(lineorder_flat.caddress)));
    ccityVector->set(i, lineorder_flat.ccity);
    cnationVector->set(i, lineorder_flat.cnation);
    cregionVector->set(i, lineorder_flat.cregion);
    cphoneVector->set(i, StringView(lineorder_flat.cphone, strlen(lineorder_flat.cphone)));
    cmktsegmentVector->set(i, StringView(lineorder_flat.cmktsegment, strlen(lineorder_flat.cmktsegment)));
    snameVector->set(i, StringView(lineorder_flat.sname, strlen(lineorder_flat.sname)));
    saddressVector->set(i, StringView(lineorder_flat.saddress, strlen(lineorder_flat.saddress)));
    scityVector->set(i, lineorder_flat.scity);
    snationVector->set(i, lineorder_flat.snation);
    sregionVector->set(i, lineorder_flat.sregion);
    sphoneVector->set(i, StringView(lineorder_flat.sphone, strlen(lineorder_flat.sphone)));
    pnameVector->set(i, StringView(lineorder_flat.pname, strlen(lineorder_flat.pname)));
    pmfgrVector->set(i, StringView(lineorder_flat.pmfgr, strlen(lineorder_flat.pmfgr)));
    pcategoryVector->set(i, lineorder_flat.pcategory);
    pbrandVector->set(i, StringView(lineorder_flat.pbrand, strlen(lineorder_flat.pbrand)));
    pcolorVector->set(i, lineorder_flat.pcolor);
    ptypeVector->set(i, StringView(lineorder_flat.ptype, strlen(lineorder_flat.ptype)));
    psizeVector->set(i, lineorder_flat.psize);
    pcontainerVector->set(i, StringView(lineorder_flat.pcontainer, strlen(lineorder_flat.pcontainer)));
    orderyearVector->set(i, lineorder_flat.orderyear);
  }

  return std::make_shared<RowVector>(
      pool,
      lineItemRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}


RowVectorPtr genTpchPart(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto partRowType = getTableSchema(Table::TBL_PART);
  size_t vectorSize =
      getVectorSize(getRowCount(Table::TBL_PART, scaleFactor), maxRows, offset);
  auto children = allocateVectors(partRowType, vectorSize, pool);

  auto partKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto mfgrVector = children[2]->asFlatVector<StringView>();
  auto brandVector = children[3]->asFlatVector<StringView>();
  auto typeVector = children[4]->asFlatVector<StringView>();
  auto sizeVector = children[5]->asFlatVector<int32_t>();
  auto containerVector = children[6]->asFlatVector<StringView>();
  auto retailPriceVector = children[7]->asFlatVector<double>();
  auto commentVector = children[8]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initPart(offset);
  part_t part;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genPart(i + offset + 1, part);

    partKeyVector->set(i, part.partkey);
    nameVector->set(i, StringView(part.name, strlen(part.name)));
    mfgrVector->set(i, StringView(part.mfgr, strlen(part.mfgr)));
    brandVector->set(i, StringView(part.brand, strlen(part.brand)));
    typeVector->set(i, StringView(part.type, part.tlen));
    sizeVector->set(i, part.size);
    containerVector->set(i, StringView(part.container, strlen(part.container)));
    retailPriceVector->set(i, decimalToDouble(part.retailprice));
    commentVector->set(i, StringView(part.comment, part.clen));
  }
  return std::make_shared<RowVector>(
      pool, partRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchSupplier(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto supplierRowType = getTableSchema(Table::TBL_SUPPLIER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_SUPPLIER, scaleFactor), maxRows, offset);
  auto children = allocateVectors(supplierRowType, vectorSize, pool);

  auto suppKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto addressVector = children[2]->asFlatVector<StringView>();
  auto nationKeyVector = children[3]->asFlatVector<int64_t>();
  auto phoneVector = children[4]->asFlatVector<StringView>();
  auto acctbalVector = children[5]->asFlatVector<double>();
  auto commentVector = children[6]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initSupplier(offset);
  supplier_t supp;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genSupplier(i + offset + 1, supp);

    suppKeyVector->set(i, supp.suppkey);
    nameVector->set(i, StringView(supp.name, strlen(supp.name)));
    addressVector->set(i, StringView(supp.address, supp.alen));
    nationKeyVector->set(i, supp.nation_code);
    phoneVector->set(i, StringView(supp.phone, strlen(supp.phone)));
    acctbalVector->set(i, decimalToDouble(supp.acctbal));
    commentVector->set(i, StringView(supp.comment, supp.clen));
  }
  return std::make_shared<RowVector>(
      pool,
      supplierRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchPartSupp(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto partSuppRowType = getTableSchema(Table::TBL_PARTSUPP);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_PARTSUPP, scaleFactor), maxRows, offset);
  auto children = allocateVectors(partSuppRowType, vectorSize, pool);

  auto partKeyVector = children[0]->asFlatVector<int64_t>();
  auto suppKeyVector = children[1]->asFlatVector<int64_t>();
  auto availQtyVector = children[2]->asFlatVector<int32_t>();
  auto supplyCostVector = children[3]->asFlatVector<double>();
  auto commentVector = children[4]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  part_t part;

  // The iteration logic is a bit more complicated as partsupp records are
  // generated using mk_part(), which returns a vector of 4 (SUPP_PER_PART)
  // partsupp record at a time. So we need to align the user's requested window
  // (maxRows, offset), with the 4-at-a-time record window provided by DBGEN.
  size_t partIdx = offset / SUPP_PER_PART;
  size_t partSuppIdx = offset % SUPP_PER_PART;
  size_t partSuppCount = 0;

  dbgenIt.initPart(partIdx);

  do {
    dbgenIt.genPart(partIdx + 1, part);

    while ((partSuppIdx < SUPP_PER_PART) && (partSuppCount < vectorSize)) {
      const auto& partSupp = part.s[partSuppIdx];

      partKeyVector->set(partSuppCount, partSupp.partkey);
      suppKeyVector->set(partSuppCount, partSupp.suppkey);
      availQtyVector->set(partSuppCount, partSupp.qty);
      supplyCostVector->set(partSuppCount, decimalToDouble(partSupp.scost));
      commentVector->set(
          partSuppCount, StringView(partSupp.comment, partSupp.clen));

      ++partSuppIdx;
      ++partSuppCount;
    }
    partSuppIdx = 0;
    ++partIdx;

  } while (partSuppCount < vectorSize);

  VELOX_CHECK_EQ(partSuppCount, vectorSize);
  return std::make_shared<RowVector>(
      pool,
      partSuppRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchCustomer(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto customerRowType = getTableSchema(Table::TBL_CUSTOMER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_CUSTOMER, scaleFactor), maxRows, offset);
  auto children = allocateVectors(customerRowType, vectorSize, pool);

  auto custKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto addressVector = children[2]->asFlatVector<StringView>();
  auto nationKeyVector = children[3]->asFlatVector<int64_t>();
  auto phoneVector = children[4]->asFlatVector<StringView>();
  auto acctBalVector = children[5]->asFlatVector<double>();
  auto mktSegmentVector = children[6]->asFlatVector<StringView>();
  auto commentVector = children[7]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initCustomer(offset);
  customer_t cust;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genCustomer(i + offset + 1, cust);

    custKeyVector->set(i, cust.custkey);
    nameVector->set(i, StringView(cust.name, strlen(cust.name)));
    addressVector->set(i, StringView(cust.address, cust.alen));
    nationKeyVector->set(i, cust.nation_code);
    phoneVector->set(i, StringView(cust.phone, strlen(cust.phone)));
    acctBalVector->set(i, decimalToDouble(cust.acctbal));
    mktSegmentVector->set(
        i, StringView(cust.mktsegment, strlen(cust.mktsegment)));
    commentVector->set(i, StringView(cust.comment, cust.clen));
  }
  return std::make_shared<RowVector>(
      pool,
      customerRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchNation(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto nationRowType = getTableSchema(Table::TBL_NATION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_NATION, scaleFactor), maxRows, offset);
  auto children = allocateVectors(nationRowType, vectorSize, pool);

  auto nationKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto regionKeyVector = children[2]->asFlatVector<int64_t>();
  auto commentVector = children[3]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initNation(offset);
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genNation(i + offset + 1, code);

    nationKeyVector->set(i, code.code);
    nameVector->set(i, StringView(code.text, strlen(code.text)));
    regionKeyVector->set(i, code.join);
    commentVector->set(i, StringView(code.comment, code.clen));
  }
  return std::make_shared<RowVector>(
      pool, nationRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchRegion(
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto regionRowType = getTableSchema(Table::TBL_REGION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_REGION, scaleFactor), maxRows, offset);
  auto children = allocateVectors(regionRowType, vectorSize, pool);

  auto regionKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto commentVector = children[2]->asFlatVector<StringView>();

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initRegion(offset);
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genRegion(i + offset + 1, code);

    regionKeyVector->set(i, code.code);
    nameVector->set(i, StringView(code.text, strlen(code.text)));
    commentVector->set(i, StringView(code.comment, code.clen));
  }
  return std::make_shared<RowVector>(
      pool, regionRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

} // namespace facebook::velox::tpch
