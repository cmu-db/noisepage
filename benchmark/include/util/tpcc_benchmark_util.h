#pragma once

#include <chrono>  // NOLINT
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/schemas.h"
#include "tpcc/util.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "util/random_test_util.h"
#include "util/transaction_benchmark_util.h"

// TODO(Matt): it seems many fields can by smaller than INTEGER

namespace terrier::tpcc {

constexpr uint16_t num_districts_per_warehouse_ = 10;
constexpr uint16_t num_customers_per_district_ = 3000;

template <class Random>
class TPCC {
 public:
  explicit TPCC(transaction::TransactionManager *txn_manager, storage::BlockStore *store, Random *generator)
      : txn_manager_(txn_manager),
        store_(store),
        generator_(generator),
        oid_counter(0),
        num_warehouses_(4),
        item_schema_(Schemas::BuildItemSchema(&oid_counter)),
        warehouse_schema_(Schemas::BuildWarehouseSchema(&oid_counter)),
        stock_schema_(Schemas::BuildStockSchema(&oid_counter)),
        district_schema_(Schemas::BuildDistrictSchema(&oid_counter)),
        customer_schema_(Schemas::BuildCustomerSchema(&oid_counter)),
        history_schema_(Schemas::BuildHistorySchema(&oid_counter)),
        new_order_schema_(Schemas::BuildNewOrderSchema(&oid_counter)),
        order_schema_(Schemas::BuildOrderSchema(&oid_counter)),
        order_line_schema_(Schemas::BuildOrderLineSchema(&oid_counter)),
        item_(new storage::SqlTable(store_, item_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        warehouse_(new storage::SqlTable(store_, warehouse_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        stock_(new storage::SqlTable(store_, stock_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        district_(new storage::SqlTable(store_, district_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        customer_(new storage::SqlTable(store_, customer_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        history_(new storage::SqlTable(store_, history_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        new_order_(new storage::SqlTable(store_, new_order_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        order_(new storage::SqlTable(store_, order_schema_, static_cast<catalog::table_oid_t>(++oid_counter))),
        order_line_(new storage::SqlTable(store_, order_line_schema_, static_cast<catalog::table_oid_t>(++oid_counter)))

  {
    PopulateTables();
  }

  ~TPCC() {
    delete item_;
    delete warehouse_;
    delete stock_;
    delete district_;
    delete customer_;
    delete history_;
    delete new_order_;
    delete order_;
    delete order_line_;
  }

 private:
  static std::vector<catalog::col_oid_t> AllColOidsForSchema(const catalog::Schema &schema) {
    const auto &cols = schema.GetColumns();
    std::vector<catalog::col_oid_t> col_oids;
    col_oids.reserve(cols.size());
    for (const auto &col : cols) {
      col_oids.emplace_back(col.GetOid());
    }
    return col_oids;
  }

  static uint64_t Timestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  template <typename T>
  static void SetPRAttribute(const catalog::Schema &schema, const uint32_t col_offset,
                             const storage::ProjectionMap &projection_map, storage::ProjectedRow *const pr, T value) {
    TERRIER_ASSERT((schema.GetColumn(col_offset).GetAttrSize() & INT8_MAX) == sizeof(T), "Invalid attribute size.");
    const auto col_oid = schema.GetColumn(col_offset).GetOid();
    const auto attr_offset = projection_map.at(col_oid);
    auto *const attr = pr->AccessForceNotNull(attr_offset);
    *reinterpret_cast<T *>(attr) = value;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildItemTuple(const int32_t i_id, const bool original, byte *const buffer,
                                        const storage::ProjectedRowInitializer &pr_initializer,
                                        const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(i_id >= 1 && i_id <= 100000, "Invalid i_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // I_ID unique within [100,000]
    TERRIER_ASSERT(item_schema_.GetColumn(col_offset).GetName() == "I_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(item_schema_, col_offset++, projection_map, pr, i_id);

    // I_IM_ID random within [1 .. 10,000]
    TERRIER_ASSERT(item_schema_.GetColumn(col_offset).GetName() == "I_IM_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(item_schema_, col_offset++, projection_map, pr,
                            RandomUtil::RandomWithin<int32_t>(1, 10000, 0, generator_));

    // I_NAME random a-string [14 .. 24]
    TERRIER_ASSERT(item_schema_.GetColumn(col_offset).GetName() == "I_NAME", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(item_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(14, 24, false, generator_));

    // I_PRICE random within [1.00 .. 100.00]
    TERRIER_ASSERT(item_schema_.GetColumn(col_offset).GetName() == "I_PRICE", "Wrong attribute.");
    SetPRAttribute<double>(item_schema_, col_offset++, projection_map, pr,
                           RandomUtil::RandomWithin<double>(100, 10000, 2, generator_));

    // I_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within I_DATA
    TERRIER_ASSERT(item_schema_.GetColumn(col_offset).GetName() == "I_DATA", "Wrong attribute.");
    if (original) {
      SetPRAttribute<storage::VarlenEntry>(item_schema_, col_offset++, projection_map, pr,
                                           RandomUtil::RandomOriginalVarlenEntry(26, 50, generator_));
    } else {
      SetPRAttribute<storage::VarlenEntry>(item_schema_, col_offset++, projection_map, pr,
                                           RandomUtil::RandomAlphaNumericVarlenEntry(26, 50, false, generator_));
    }

    TERRIER_ASSERT(col_offset == item_schema_.GetColumns().size(), "Didn't get every attribute for Item tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildWarehouseTuple(const int32_t w_id, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // W_ID unique within [number_of_configured_warehouses]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(warehouse_schema_, col_offset++, projection_map, pr, w_id);

    // W_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_NAME", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(6, 10, false, generator_));

    // W_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_STREET_1", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // W_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_STREET_2", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // W_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_CITY", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // W_STATE random a-string of 2 letters
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_STATE", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(2, 2, false, generator_));

    // W_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_ZIP", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(warehouse_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomZipVarlenEntry(generator_));

    // W_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_TAX", "Wrong attribute.");
    SetPRAttribute<double>(warehouse_schema_, col_offset++, projection_map, pr,
                           RandomUtil::RandomWithin<double>(0, 2000, 4, generator_));

    // W_YTD = 300,000.00
    TERRIER_ASSERT(warehouse_schema_.GetColumn(col_offset).GetName() == "W_YTD", "Wrong attribute.");
    SetPRAttribute<double>(warehouse_schema_, col_offset++, projection_map, pr, 300000.0);

    TERRIER_ASSERT(col_offset == warehouse_schema_.GetColumns().size(),
                   "Didn't get every attribute for Warehouse tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildStockTuple(const int32_t s_i_id, const int32_t w_id, const bool original,
                                         byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                         const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(s_i_id >= 1 && s_i_id <= 100000, "Invalid s_i_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // S_I_ID unique within [100,000]
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_I_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(stock_schema_, col_offset++, projection_map, pr, s_i_id);

    // S_W_ID = W_ID
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(stock_schema_, col_offset++, projection_map, pr, w_id);

    // S_QUANTITY random within [10 .. 100]
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_QUANTITY", "Wrong attribute.");
    SetPRAttribute<int16_t>(stock_schema_, col_offset++, projection_map, pr,
                            RandomUtil::RandomWithin<int16_t>(10, 100, 0, generator_));

    // S_DIST_01 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_01", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_02 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_02", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_03 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_03", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_04 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_04", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_05 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_05", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_06 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_06", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_07 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_07", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_08 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_08", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_09 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_09", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_DIST_10 random a-string of 24 letters
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DIST_10", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    // S_YTD = 0
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_YTD", "Wrong attribute.");
    SetPRAttribute<int32_t>(stock_schema_, col_offset++, projection_map, pr, 0);

    // S_ORDER_CNT = 0
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_ORDER_CNT", "Wrong attribute.");
    SetPRAttribute<int16_t>(stock_schema_, col_offset++, projection_map, pr, 0);

    // S_REMOTE_CNT = 0
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_REMOTE_CNT", "Wrong attribute.");
    SetPRAttribute<int16_t>(stock_schema_, col_offset++, projection_map, pr, 0);

    // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within S_DATA
    TERRIER_ASSERT(stock_schema_.GetColumn(col_offset).GetName() == "S_DATA", "Wrong attribute.");
    if (original) {
      SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                           RandomUtil::RandomOriginalVarlenEntry(26, 50, generator_));
    } else {
      SetPRAttribute<storage::VarlenEntry>(stock_schema_, col_offset++, projection_map, pr,
                                           RandomUtil::RandomAlphaNumericVarlenEntry(26, 50, false, generator_));
    }

    TERRIER_ASSERT(col_offset == stock_schema_.GetColumns().size(), "Didn't get every attribute for Stock tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildDistrictTuple(const int32_t d_id, const int32_t w_id, byte *const buffer,
                                            const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // D_ID unique within [10]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(district_schema_, col_offset++, projection_map, pr, d_id);

    // D_W_ID = W_ID
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(district_schema_, col_offset++, projection_map, pr, w_id);

    // D_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_NAME", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(6, 10, false, generator_));

    // D_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_STREET_1", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // D_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_STREET_2", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // D_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_CITY", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // D_STATE random a-string of 2 letters
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_STATE", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(2, 2, false, generator_));

    // D_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_ZIP", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(district_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomZipVarlenEntry(generator_));

    // D_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_TAX", "Wrong attribute.");
    SetPRAttribute<double>(district_schema_, col_offset++, projection_map, pr,
                           RandomUtil::RandomWithin<double>(0, 2000, 4, generator_));

    // D_YTD = 30,000.00
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_YTD", "Wrong attribute.");
    SetPRAttribute<double>(district_schema_, col_offset++, projection_map, pr, 30000.0);

    // D_NEXT_O_ID = 3,001
    TERRIER_ASSERT(district_schema_.GetColumn(col_offset).GetName() == "D_NEXT_O_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(district_schema_, col_offset++, projection_map, pr, 3001);

    TERRIER_ASSERT(col_offset == district_schema_.GetColumns().size(),
                   "Didn't get every attribute for District tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildCustomerTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                            const bool good_credit, byte *const buffer,
                                            const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(customer_schema_, col_offset++, projection_map, pr, c_id);

    // C_D_ID = D_ID
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(customer_schema_, col_offset++, projection_map, pr, d_id);

    // C_W_ID = D_W_ID
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(customer_schema_, col_offset++, projection_map, pr, w_id);

    // C_FIRST random a-string [8 .. 16]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_FIRST", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(8, 16, false, generator_));

    // C_MIDDLE = "OE"
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_MIDDLE", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(
        customer_schema_, col_offset++, projection_map, pr,
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2));

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_LAST", "Wrong attribute.");
    if (c_id <= 1000) {
      SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                           RandomUtil::RandomLastNameVarlenEntry(c_id - 1));
    } else {
      SetPRAttribute<storage::VarlenEntry>(
          customer_schema_, col_offset++, projection_map, pr,
          RandomUtil::RandomLastNameVarlenEntry(RandomUtil::NURand(255, 0, 999, generator_)));
    }

    // C_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_STREET_1", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // C_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_STREET_2", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // C_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_CITY", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(10, 20, false, generator_));

    // C_STATE random a-string of 2 letters
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_STATE", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(2, 2, false, generator_));

    // C_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_ZIP", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomZipVarlenEntry(generator_));

    // C_PHONE random n-string of 16 numbers
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_PHONE", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(16, 16, true, generator_));

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_SINCE", "Wrong attribute.");
    SetPRAttribute<uint64_t>(customer_schema_, col_offset++, projection_map, pr, Timestamp());

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_CREDIT", "Wrong attribute.");
    if (good_credit) {
      SetPRAttribute<storage::VarlenEntry>(
          customer_schema_, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2));
    } else {
      SetPRAttribute<storage::VarlenEntry>(
          customer_schema_, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2));
    }

    // C_CREDIT_LIM = 50,000.00
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_CREDIT_LIM", "Wrong attribute.");
    SetPRAttribute<double>(customer_schema_, col_offset++, projection_map, pr, 50000.0);

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_DISCOUNT", "Wrong attribute.");
    SetPRAttribute<double>(customer_schema_, col_offset++, projection_map, pr,
                           RandomUtil::RandomWithin<double>(0, 5000, 4, generator_));

    // C_BALANCE = -10.00
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_BALANCE", "Wrong attribute.");
    SetPRAttribute<double>(customer_schema_, col_offset++, projection_map, pr, -10.0);

    // C_YTD_PAYMENT = 10.00
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_YTD_PAYMENT", "Wrong attribute.");
    SetPRAttribute<double>(customer_schema_, col_offset++, projection_map, pr, 10.0);

    // C_PAYMENT_CNT = 1
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_PAYMENT_CNT", "Wrong attribute.");
    SetPRAttribute<int16_t>(customer_schema_, col_offset++, projection_map, pr, 1);

    // C_DELIVERY_CNT = 0
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_DELIVERY_CNT", "Wrong attribute.");
    SetPRAttribute<int16_t>(customer_schema_, col_offset++, projection_map, pr, 0);

    // C_DATA random a-string [300 .. 500]
    TERRIER_ASSERT(customer_schema_.GetColumn(col_offset).GetName() == "C_DATA", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(customer_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(300, 500, false, generator_));

    TERRIER_ASSERT(col_offset == customer_schema_.GetColumns().size(),
                   "Didn't get every attribute for Customer tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildHistoryTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                           byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                           const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // H_C_ID = C_ID
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_C_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(history_schema_, col_offset++, projection_map, pr, c_id);

    // H_C_D_ID = D_ID
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_C_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(history_schema_, col_offset++, projection_map, pr, d_id);

    // H_C_W_ID = W_ID
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_C_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(history_schema_, col_offset++, projection_map, pr, w_id);

    // H_D_ID = D_ID
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(history_schema_, col_offset++, projection_map, pr, d_id);

    // H_W_ID = W_ID
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(history_schema_, col_offset++, projection_map, pr, w_id);

    // H_DATE current date and time
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_DATE", "Wrong attribute.");
    SetPRAttribute<uint64_t>(history_schema_, col_offset++, projection_map, pr, Timestamp());

    // H_AMOUNT = 10.00
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_AMOUNT", "Wrong attribute.");
    SetPRAttribute<double>(history_schema_, col_offset++, projection_map, pr, 10.0);

    // H_DATA random a-string [12 .. 24]
    TERRIER_ASSERT(history_schema_.GetColumn(col_offset).GetName() == "H_DATA", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(history_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(12, 24, false, generator_));

    TERRIER_ASSERT(col_offset == history_schema_.GetColumns().size(), "Didn't get every attribute for History tuple.");

    return pr;
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildNewOrderTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                            byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                            const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 2101 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // NO_O_ID = O_ID
    TERRIER_ASSERT(new_order_schema_.GetColumn(col_offset).GetName() == "NO_O_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(new_order_schema_, col_offset++, projection_map, pr, o_id);

    // NO_D_ID = D_ID
    TERRIER_ASSERT(new_order_schema_.GetColumn(col_offset).GetName() == "NO_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(new_order_schema_, col_offset++, projection_map, pr, d_id);

    // NO_W_ID = W_ID
    TERRIER_ASSERT(new_order_schema_.GetColumn(col_offset).GetName() == "NO_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(new_order_schema_, col_offset++, projection_map, pr, w_id);

    TERRIER_ASSERT(col_offset == new_order_schema_.GetColumns().size(),
                   "Didn't get every attribute for New Order tuple.");

    return pr;
  }

  struct OrderTupleResults {
    storage::ProjectedRow *const pr;
    uint64_t o_entry_d;
    int8_t o_ol_cnt;
  };

  // 4.3.3.1
  OrderTupleResults BuildOrderTuple(const int32_t o_id, const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                    byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
                                    const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // O_ID unique within [3,000]
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, o_id);

    // O_D_ID = D_ID
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, d_id);

    // O_W_ID = W_ID
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, w_id);

    // O_C_ID selected sequentially from a random permutation of [1 .. 3,000]
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_C_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, c_id);

    // O_ENTRY_D current date/ time given by the operating system
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_ENTRY_D", "Wrong attribute.");
    const uint64_t entry_d = Timestamp();
    SetPRAttribute<uint64_t>(order_schema_, col_offset++, projection_map, pr, entry_d);

    // O_CARRIER_ID random within [1 .. 10] if O_ID < 2,101, null otherwise
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_CARRIER_ID", "Wrong attribute.");
    const auto col_oid = order_schema_.GetColumn(col_offset++).GetOid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<int32_t *>(attr) = RandomUtil::RandomWithin<int32_t>(1, 10, 0, generator_);
    } else {
      pr->SetNull(attr_offset);
    }

    // O_OL_CNT random within [5 .. 15]
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_OL_CNT", "Wrong attribute.");
    const auto ol_cnt = RandomUtil::RandomWithin<int32_t>(5, 15, 0, generator_);
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, ol_cnt);

    // O_ALL_LOCAL = 1
    TERRIER_ASSERT(order_schema_.GetColumn(col_offset).GetName() == "O_ALL_LOCAL", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_schema_, col_offset++, projection_map, pr, 1);

    TERRIER_ASSERT(col_offset == order_schema_.GetColumns().size(), "Didn't get every attribute for Order tuple.");

    return {pr, entry_d, static_cast<int8_t>(ol_cnt)};
  }

  // 4.3.3.1
  storage::ProjectedRow *BuildOrderLineTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                             const int32_t ol_number, const uint64_t o_entry_d, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const storage::ProjectionMap &projection_map) const {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // OL_O_ID = O_ID
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_O_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, o_id);

    // OL_D_ID = D_ID
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_D_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, d_id);

    // OL_W_ID = W_ID
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, w_id);

    // OL_NUMBER unique within [O_OL_CNT]
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_NUMBER", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, ol_number);

    // OL_I_ID random within [1 .. 100,000]
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_I_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr,
                            RandomUtil::RandomWithin<int32_t>(1, 100000, 0, generator_));

    // OL_SUPPLY_W_ID = W_ID
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_SUPPLY_W_ID", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, w_id);

    // OL_DELIVERY_D = O_ENTRY_D if OL_O_ID < 2,101, null otherwise
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_DELIVERY_D", "Wrong attribute.");
    const auto col_oid = order_line_schema_.GetColumn(col_offset++).GetOid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<uint64_t *>(attr) = o_entry_d;
    } else {
      pr->SetNull(attr_offset);
    }

    // OL_QUANTITY = 5
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_QUANTITY", "Wrong attribute.");
    SetPRAttribute<int32_t>(order_line_schema_, col_offset++, projection_map, pr, 5);

    // OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_AMOUNT", "Wrong attribute.");
    if (o_id < 2101) {
      SetPRAttribute<double>(order_line_schema_, col_offset++, projection_map, pr, 0.0);
    } else {
      SetPRAttribute<double>(order_line_schema_, col_offset++, projection_map, pr,
                             RandomUtil::RandomWithin<double>(1, 999999, 2, generator_));
    }

    // OL_DIST_INFO random a-string of 24 letters
    TERRIER_ASSERT(order_line_schema_.GetColumn(col_offset).GetName() == "OL_DIST_INFO", "Wrong attribute.");
    SetPRAttribute<storage::VarlenEntry>(order_line_schema_, col_offset++, projection_map, pr,
                                         RandomUtil::RandomAlphaNumericVarlenEntry(24, 24, false, generator_));

    TERRIER_ASSERT(col_offset == order_line_schema_.GetColumns().size(),
                   "Didn't get every attribute for Order Line tuple.");

    return pr;
  }

  void PopulateTables() {
    TERRIER_ASSERT(txn_manager_ != nullptr, "TransactionManager does not exist.");

    // Item
    const auto item_col_oids = AllColOidsForSchema(item_schema_);
    const auto item_pr_initializer = item_->InitializerForProjectedRow(item_col_oids).first;
    const auto item_pr_map = item_->InitializerForProjectedRow(item_col_oids).second;
    auto *const item_buffer(common::AllocationUtil::AllocateAligned(item_pr_initializer.ProjectedRowSize()));

    // Warehouse
    const auto warehouse_col_oids = AllColOidsForSchema(warehouse_schema_);
    const auto warehouse_pr_initializer = warehouse_->InitializerForProjectedRow(warehouse_col_oids).first;
    const auto warehouse_pr_map = warehouse_->InitializerForProjectedRow(warehouse_col_oids).second;
    auto *const warehouse_buffer(common::AllocationUtil::AllocateAligned(warehouse_pr_initializer.ProjectedRowSize()));

    // Stock
    const auto stock_col_oids = AllColOidsForSchema(stock_schema_);
    const auto stock_pr_initializer = stock_->InitializerForProjectedRow(stock_col_oids).first;
    const auto stock_pr_map = stock_->InitializerForProjectedRow(stock_col_oids).second;
    auto *const stock_buffer(common::AllocationUtil::AllocateAligned(stock_pr_initializer.ProjectedRowSize()));

    // District
    const auto district_col_oids = AllColOidsForSchema(district_schema_);
    const auto district_pr_initializer = district_->InitializerForProjectedRow(district_col_oids).first;
    const auto district_pr_map = district_->InitializerForProjectedRow(district_col_oids).second;
    auto *const district_buffer(common::AllocationUtil::AllocateAligned(district_pr_initializer.ProjectedRowSize()));

    // Customer
    const auto customer_col_oids = AllColOidsForSchema(customer_schema_);
    const auto customer_pr_initializer = customer_->InitializerForProjectedRow(customer_col_oids).first;
    const auto customer_pr_map = customer_->InitializerForProjectedRow(customer_col_oids).second;
    auto *const customer_buffer(common::AllocationUtil::AllocateAligned(customer_pr_initializer.ProjectedRowSize()));

    // History
    const auto history_col_oids = AllColOidsForSchema(history_schema_);
    const auto history_pr_initializer = history_->InitializerForProjectedRow(history_col_oids).first;
    const auto history_pr_map = history_->InitializerForProjectedRow(history_col_oids).second;
    auto *const history_buffer(common::AllocationUtil::AllocateAligned(history_pr_initializer.ProjectedRowSize()));

    // Order
    const auto order_col_oids = AllColOidsForSchema(order_schema_);
    const auto order_pr_initializer = order_->InitializerForProjectedRow(order_col_oids).first;
    const auto order_pr_map = order_->InitializerForProjectedRow(order_col_oids).second;
    auto *const order_buffer(common::AllocationUtil::AllocateAligned(order_pr_initializer.ProjectedRowSize()));

    // New Order
    const auto new_order_col_oids = AllColOidsForSchema(new_order_schema_);
    const auto new_order_pr_initializer = new_order_->InitializerForProjectedRow(new_order_col_oids).first;
    const auto new_order_pr_map = new_order_->InitializerForProjectedRow(new_order_col_oids).second;
    auto *const new_order_buffer(common::AllocationUtil::AllocateAligned(new_order_pr_initializer.ProjectedRowSize()));

    // Order Line
    const auto order_line_col_oids = AllColOidsForSchema(order_line_schema_);
    const auto order_line_pr_initializer = order_line_->InitializerForProjectedRow(order_line_col_oids).first;
    const auto order_line_pr_map = order_line_->InitializerForProjectedRow(order_line_col_oids).second;
    auto *const order_line_buffer(
        common::AllocationUtil::AllocateAligned(order_line_pr_initializer.ProjectedRowSize()));

    auto *const txn = txn_manager_->BeginTransaction();

    // generate booleans to represent ORIGINAL for item and stock. 10% are ORIGINAL (true), and then shuffled
    std::vector<bool> original;
    original.reserve(100000);
    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      original.emplace_back(i_id < 10000);
    }
    std::shuffle(original.begin(), original.end(), *generator_);

    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      // 100,000 rows in the ITEM table
      item_->Insert(txn, *BuildItemTuple(i_id + 1, original[i_id], item_buffer, item_pr_initializer, item_pr_map));
    }

    for (uint32_t w_id = 0; w_id < num_warehouses_; w_id++) {
      // 1 row in the WAREHOUSE table for each configured warehouse
      warehouse_->Insert(txn,
                         *BuildWarehouseTuple(w_id + 1, warehouse_buffer, warehouse_pr_initializer, warehouse_pr_map));

      // shuffle the ORIGINAL vector again since we reuse it for stock table
      std::shuffle(original.begin(), original.end(), *generator_);

      for (uint32_t s_i_id = 0; s_i_id < 100000; s_i_id++) {
        // For each row in the WAREHOUSE table:
        // 100,000 rows in the STOCK table
        stock_->Insert(txn, *BuildStockTuple(s_i_id + 1, w_id + 1, original[s_i_id], stock_buffer, stock_pr_initializer,
                                             stock_pr_map));
      }

      for (uint32_t d_id = 0; d_id < num_districts_per_warehouse_; d_id++) {
        // For each row in the WAREHOUSE table:
        // 10 rows in the DISTRICT table
        district_->Insert(
            txn, *BuildDistrictTuple(d_id + 1, w_id + 1, district_buffer, district_pr_initializer, district_pr_map));

        // O_C_ID selected sequentially from a random permutation of [1 .. 3,000] for Order table
        std::vector<uint32_t> o_c_ids;
        o_c_ids.reserve(num_customers_per_district_);
        // generate booleans to represent GC or BC for customers. 90% are GC (true), and then shuffled
        std::vector<bool> c_credit;
        c_credit.reserve(num_customers_per_district_);
        for (uint32_t c_id = 0; c_id < num_customers_per_district_; c_id++) {
          c_credit.emplace_back(c_id < num_customers_per_district_ / 10);
          o_c_ids.emplace_back(c_id + 1);
        }
        std::shuffle(c_credit.begin(), c_credit.end(), *generator_);
        std::shuffle(o_c_ids.begin(), o_c_ids.end(), *generator_);

        for (uint32_t c_id = 0; c_id < num_customers_per_district_; c_id++) {
          // For each row in the DISTRICT table:
          // 3,000 rows in the CUSTOMER table
          customer_->Insert(txn, *BuildCustomerTuple(c_id + 1, d_id + 1, w_id + 1, c_credit[c_id], customer_buffer,
                                                     customer_pr_initializer, customer_pr_map));

          // For each row in the CUSTOMER table:
          // 1 row in the HISTORY table
          history_->Insert(txn, *BuildHistoryTuple(c_id + 1, d_id + 1, w_id + 1, history_buffer, history_pr_initializer,
                                                   history_pr_map));

          // For each row in the DISTRICT table:
          // 3,000 rows in the ORDER table
          const auto o_id = c_id;
          const auto order_results = BuildOrderTuple(o_id + 1, o_c_ids[c_id], d_id + 1, w_id + 1, order_buffer,
                                                     order_pr_initializer, order_pr_map);
          order_->Insert(txn, *(order_results.pr));

          // For each row in the ORDER table:
          // A number of rows in the ORDER-LINE table equal to O_OL_CNT, generated according to the rules for input
          // data generation of the New-Order transaction (see Clause 2.4.1)
          for (int8_t ol_number = 0; ol_number < order_results.o_ol_cnt; ol_number++) {
            order_line_->Insert(
                txn, *BuildOrderLineTuple(o_id + 1, d_id + 1, w_id + 1, ol_number + 1, order_results.o_entry_d,
                                          order_line_buffer, order_line_pr_initializer, order_line_pr_map));
          }

          // For each row in the DISTRICT table:
          // 900 rows in the NEW-ORDER table corresponding to the last 900 rows in the ORDER table for that district
          // (i.e., with NO_O_ID between 2,101 and 3,000)
          if (o_id + 1 >= 2101) {
            new_order_->Insert(txn, *BuildNewOrderTuple(o_id + 1, d_id + 1, w_id + 1, new_order_buffer,
                                                        new_order_pr_initializer, new_order_pr_map));
          }
        }
      }
    }

    txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    delete[] item_buffer;
    delete[] warehouse_buffer;
    delete[] stock_buffer;
    delete[] district_buffer;
    delete[] customer_buffer;
    delete[] history_buffer;
    delete[] order_buffer;
    delete[] new_order_buffer;
    delete[] order_line_buffer;
  }

  transaction::TransactionManager *const txn_manager_;
  storage::BlockStore *const store_;
  Random *const generator_;

  uint64_t oid_counter;

  const uint32_t num_warehouses_;  // TODO(Matt): don't hardcode this

  const catalog::Schema item_schema_;
  const catalog::Schema warehouse_schema_;
  const catalog::Schema stock_schema_;
  const catalog::Schema district_schema_;
  const catalog::Schema customer_schema_;
  const catalog::Schema history_schema_;
  const catalog::Schema new_order_schema_;
  const catalog::Schema order_schema_;
  const catalog::Schema order_line_schema_;

  storage::SqlTable *const item_;
  storage::SqlTable *const warehouse_;
  storage::SqlTable *const stock_;
  storage::SqlTable *const district_;
  storage::SqlTable *const customer_;
  storage::SqlTable *const history_;
  storage::SqlTable *const new_order_;
  storage::SqlTable *const order_;
  storage::SqlTable *const order_line_;
};

}  // namespace terrier::tpcc
