#pragma once

#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/projected_row.h"
#include "tpcc/util.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

constexpr uint32_t num_warehouses_ = 4;
constexpr uint16_t num_districts_per_warehouse_ = 10;
constexpr uint16_t num_customers_per_district_ = 3000;

// 4.3.3.1
struct Loader {
  Loader() = delete;

  template <class Random>
  static void PopulateTables(transaction::TransactionManager *const txn_manager, Random *const generator,
                             storage::SqlTable *const item, storage::SqlTable *const warehouse,
                             storage::SqlTable *const stock, storage::SqlTable *const district,
                             storage::SqlTable *const customer, storage::SqlTable *const history,
                             storage::SqlTable *const new_order, storage::SqlTable *const order,
                             storage::SqlTable *const order_line, const catalog::Schema &item_schema,
                             const catalog::Schema &warehouse_schema, const catalog::Schema &stock_schema,
                             const catalog::Schema &district_schema, const catalog::Schema &customer_schema,
                             const catalog::Schema &history_schema, const catalog::Schema &new_order_schema,
                             const catalog::Schema &order_schema, const catalog::Schema &order_line_schema) {
    TERRIER_ASSERT(txn_manager != nullptr, "TransactionManager does not exist.");

    // Item
    const auto item_col_oids = Util::AllColOidsForSchema(item_schema);
    const auto item_pr_initializer = item->InitializerForProjectedRow(item_col_oids).first;
    const auto item_pr_map = item->InitializerForProjectedRow(item_col_oids).second;
    auto *const item_buffer(common::AllocationUtil::AllocateAligned(item_pr_initializer.ProjectedRowSize()));

    // Warehouse
    const auto warehouse_col_oids = Util::AllColOidsForSchema(warehouse_schema);
    const auto warehouse_pr_initializer = warehouse->InitializerForProjectedRow(warehouse_col_oids).first;
    const auto warehouse_pr_map = warehouse->InitializerForProjectedRow(warehouse_col_oids).second;
    auto *const warehouse_buffer(common::AllocationUtil::AllocateAligned(warehouse_pr_initializer.ProjectedRowSize()));

    // Stock
    const auto stock_col_oids = Util::AllColOidsForSchema(stock_schema);
    const auto stock_pr_initializer = stock->InitializerForProjectedRow(stock_col_oids).first;
    const auto stock_pr_map = stock->InitializerForProjectedRow(stock_col_oids).second;
    auto *const stock_buffer(common::AllocationUtil::AllocateAligned(stock_pr_initializer.ProjectedRowSize()));

    // District
    const auto district_col_oids = Util::AllColOidsForSchema(district_schema);
    const auto district_pr_initializer = district->InitializerForProjectedRow(district_col_oids).first;
    const auto district_pr_map = district->InitializerForProjectedRow(district_col_oids).second;
    auto *const district_buffer(common::AllocationUtil::AllocateAligned(district_pr_initializer.ProjectedRowSize()));

    // Customer
    const auto customer_col_oids = Util::AllColOidsForSchema(customer_schema);
    const auto customer_pr_initializer = customer->InitializerForProjectedRow(customer_col_oids).first;
    const auto customer_pr_map = customer->InitializerForProjectedRow(customer_col_oids).second;
    auto *const customer_buffer(common::AllocationUtil::AllocateAligned(customer_pr_initializer.ProjectedRowSize()));

    // History
    const auto history_col_oids = Util::AllColOidsForSchema(history_schema);
    const auto history_pr_initializer = history->InitializerForProjectedRow(history_col_oids).first;
    const auto history_pr_map = history->InitializerForProjectedRow(history_col_oids).second;
    auto *const history_buffer(common::AllocationUtil::AllocateAligned(history_pr_initializer.ProjectedRowSize()));

    // Order
    const auto order_col_oids = Util::AllColOidsForSchema(order_schema);
    const auto order_pr_initializer = order->InitializerForProjectedRow(order_col_oids).first;
    const auto order_pr_map = order->InitializerForProjectedRow(order_col_oids).second;
    auto *const order_buffer(common::AllocationUtil::AllocateAligned(order_pr_initializer.ProjectedRowSize()));

    // New Order
    const auto new_order_col_oids = Util::AllColOidsForSchema(new_order_schema);
    const auto new_order_pr_initializer = new_order->InitializerForProjectedRow(new_order_col_oids).first;
    const auto new_order_pr_map = new_order->InitializerForProjectedRow(new_order_col_oids).second;
    auto *const new_order_buffer(common::AllocationUtil::AllocateAligned(new_order_pr_initializer.ProjectedRowSize()));

    // Order Line
    const auto order_line_col_oids = Util::AllColOidsForSchema(order_line_schema);
    const auto order_line_pr_initializer = order_line->InitializerForProjectedRow(order_line_col_oids).first;
    const auto order_line_pr_map = order_line->InitializerForProjectedRow(order_line_col_oids).second;
    auto *const order_line_buffer(
        common::AllocationUtil::AllocateAligned(order_line_pr_initializer.ProjectedRowSize()));

    auto *const txn = txn_manager->BeginTransaction();

    // generate booleans to represent ORIGINAL for item and stock. 10% are ORIGINAL (true), and then shuffled
    std::vector<bool> original;
    original.reserve(100000);
    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      original.emplace_back(i_id < 10000);
    }
    std::shuffle(original.begin(), original.end(), *generator);

    for (uint32_t i_id = 0; i_id < 100000; i_id++) {
      // 100,000 rows in the ITEM table
      item->Insert(txn, *BuildItemTuple(i_id + 1, original[i_id], item_buffer, item_pr_initializer, item_pr_map,
                                        item_schema, generator));
    }

    for (uint32_t w_id = 0; w_id < num_warehouses_; w_id++) {
      // 1 row in the WAREHOUSE table for each configured warehouse
      warehouse->Insert(txn, *BuildWarehouseTuple(w_id + 1, warehouse_buffer, warehouse_pr_initializer,
                                                  warehouse_pr_map, warehouse_schema, generator));

      // shuffle the ORIGINAL vector again since we reuse it for stock table
      std::shuffle(original.begin(), original.end(), *generator);

      for (uint32_t s_i_id = 0; s_i_id < 100000; s_i_id++) {
        // For each row in the WAREHOUSE table:
        // 100,000 rows in the STOCK table
        stock->Insert(txn, *BuildStockTuple(s_i_id + 1, w_id + 1, original[s_i_id], stock_buffer, stock_pr_initializer,
                                            stock_pr_map, stock_schema, generator));
      }

      for (uint32_t d_id = 0; d_id < num_districts_per_warehouse_; d_id++) {
        // For each row in the WAREHOUSE table:
        // 10 rows in the DISTRICT table
        district->Insert(txn, *BuildDistrictTuple(d_id + 1, w_id + 1, district_buffer, district_pr_initializer,
                                                  district_pr_map, district_schema, generator));

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
        std::shuffle(c_credit.begin(), c_credit.end(), *generator);
        std::shuffle(o_c_ids.begin(), o_c_ids.end(), *generator);

        for (uint32_t c_id = 0; c_id < num_customers_per_district_; c_id++) {
          // For each row in the DISTRICT table:
          // 3,000 rows in the CUSTOMER table
          customer->Insert(txn,
                           *BuildCustomerTuple(c_id + 1, d_id + 1, w_id + 1, c_credit[c_id], customer_buffer,
                                               customer_pr_initializer, customer_pr_map, customer_schema, generator));

          // For each row in the CUSTOMER table:
          // 1 row in the HISTORY table
          history->Insert(txn, *BuildHistoryTuple(c_id + 1, d_id + 1, w_id + 1, history_buffer, history_pr_initializer,
                                                  history_pr_map, history_schema, generator));

          // For each row in the DISTRICT table:
          // 3,000 rows in the ORDER table
          const auto o_id = c_id;
          const auto order_results = BuildOrderTuple(o_id + 1, o_c_ids[c_id], d_id + 1, w_id + 1, order_buffer,
                                                     order_pr_initializer, order_pr_map, order_schema, generator);
          order->Insert(txn, *(order_results.pr));

          // For each row in the ORDER table:
          // A number of rows in the ORDER-LINE table equal to O_OL_CNT, generated according to the rules for input
          // data generation of the New-Order transaction (see Clause 2.4.1)
          for (int8_t ol_number = 0; ol_number < order_results.o_ol_cnt; ol_number++) {
            order_line->Insert(
                txn, *BuildOrderLineTuple(o_id + 1, d_id + 1, w_id + 1, ol_number + 1, order_results.o_entry_d,
                                          order_line_buffer, order_line_pr_initializer, order_line_pr_map,
                                          order_line_schema, generator));
          }

          // For each row in the DISTRICT table:
          // 900 rows in the NEW-ORDER table corresponding to the last 900 rows in the ORDER table for that district
          // (i.e., with NO_O_ID between 2,101 and 3,000)
          if (o_id + 1 >= 2101) {
            new_order->Insert(txn, *BuildNewOrderTuple(o_id + 1, d_id + 1, w_id + 1, new_order_buffer,
                                                       new_order_pr_initializer, new_order_pr_map, new_order_schema));
          }
        }
      }
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

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

 private:
  template <class Random>
  static storage::ProjectedRow *BuildItemTuple(const int32_t i_id, const bool original, byte *const buffer,
                                               const storage::ProjectedRowInitializer &pr_initializer,
                                               const storage::ProjectionMap &projection_map,
                                               const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(i_id >= 1 && i_id <= 100000, "Invalid i_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // I_ID unique within [100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "I_ID", "Wrong attribute.");
    Util::Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, i_id);

    // I_IM_ID random within [1 .. 10,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "I_IM_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr,
                                  Util::RandomWithin<int32_t>(1, 10000, 0, generator));

    // I_NAME random a-string [14 .. 24]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "I_NAME", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(14, 24, false, generator));

    // I_PRICE random within [1.00 .. 100.00]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "I_PRICE", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr,
                                 Util::RandomWithin<double>(100, 10000, 2, generator));

    // I_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within I_DATA
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "I_DATA", "Wrong attribute.");
    if (original) {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::OriginalVarlenEntry(26, 50, generator));
    } else {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::AlphaNumericVarlenEntry(26, 50, false, generator));
    }

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Item tuple.");

    return pr;
  }

  template <class Random>
  static storage::ProjectedRow *BuildWarehouseTuple(const int32_t w_id, byte *const buffer,
                                                    const storage::ProjectedRowInitializer &pr_initializer,
                                                    const storage::ProjectionMap &projection_map,
                                                    const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // W_ID unique within [number_of_configured_warehouses]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // W_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_NAME", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(6, 10, false, generator));

    // W_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_STREET_1", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_STREET_2", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_CITY", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_STATE", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // W_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_ZIP", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::ZipVarlenEntry(generator));

    // W_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_TAX", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr,
                                 Util::RandomWithin<double>(0, 2000, 4, generator));

    // W_YTD = 300,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "W_YTD", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 300000.0);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Warehouse tuple.");

    return pr;
  }

  template <class Random>
  static storage::ProjectedRow *BuildStockTuple(const int32_t s_i_id, const int32_t w_id, const bool original,
                                                byte *const buffer,
                                                const storage::ProjectedRowInitializer &pr_initializer,
                                                const storage::ProjectionMap &projection_map,
                                                const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(s_i_id >= 1 && s_i_id <= 100000, "Invalid s_i_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // S_I_ID unique within [100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_I_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, s_i_id);

    // S_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // S_QUANTITY random within [10 .. 100]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_QUANTITY", "Wrong attribute.");
    Util::SetPRAttribute<int16_t>(schema, col_offset++, projection_map, pr,
                                  Util::RandomWithin<int16_t>(10, 100, 0, generator));

    // S_DIST_01 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_01", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_02 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_02", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_03 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_03", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_04 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_04", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_05 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_05", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_06 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_06", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_07 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_07", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_08 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_08", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_09 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_09", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_10 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DIST_10", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_YTD = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_YTD", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, 0);

    // S_ORDER_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_ORDER_CNT", "Wrong attribute.");
    Util::SetPRAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // S_REMOTE_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_REMOTE_CNT", "Wrong attribute.");
    Util::SetPRAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within S_DATA
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "S_DATA", "Wrong attribute.");
    if (original) {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::OriginalVarlenEntry(26, 50, generator));
    } else {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::AlphaNumericVarlenEntry(26, 50, false, generator));
    }

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Stock tuple.");

    return pr;
  }

  template <class Random>
  static storage::ProjectedRow *BuildDistrictTuple(const int32_t d_id, const int32_t w_id, byte *const buffer,
                                                   const storage::ProjectedRowInitializer &pr_initializer,
                                                   const storage::ProjectionMap &projection_map,
                                                   const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // D_ID unique within [10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // D_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // D_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_NAME", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(6, 10, false, generator));

    // D_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_STREET_1", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_STREET_2", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_CITY", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_STATE", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // D_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_ZIP", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::ZipVarlenEntry(generator));

    // D_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_TAX", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr,
                                 Util::RandomWithin<double>(0, 2000, 4, generator));

    // D_YTD = 30,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_YTD", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 30000.0);

    // D_NEXT_O_ID = 3,001
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "D_NEXT_O_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, 3001);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for District tuple.");

    return pr;
  }

  template <class Random>
  static storage::ProjectedRow *BuildCustomerTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                                   const bool good_credit, byte *const buffer,
                                                   const storage::ProjectedRowInitializer &pr_initializer,
                                                   const storage::ProjectionMap &projection_map,
                                                   const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // C_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // C_W_ID = D_W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // C_FIRST random a-string [8 .. 16]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_FIRST", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(8, 16, false, generator));

    // C_MIDDLE = "OE"
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_MIDDLE", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(
        schema, col_offset++, projection_map, pr,
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2));

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_LAST", "Wrong attribute.");
    if (c_id <= 1000) {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::LastNameVarlenEntry(c_id - 1));
    } else {
      Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                 Util::LastNameVarlenEntry(Util::NURand(255, 0, 999, generator)));
    }

    // C_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_STREET_1", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_STREET_2", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_CITY", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_STATE", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // C_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_ZIP", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::ZipVarlenEntry(generator));

    // C_PHONE random n-string of 16 numbers
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_PHONE", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(16, 16, true, generator));

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_SINCE", "Wrong attribute.");
    Util::SetPRAttribute<uint64_t>(schema, col_offset++, projection_map, pr, Util::Timestamp());

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_CREDIT", "Wrong attribute.");
    if (good_credit) {
      Util::SetPRAttribute<storage::VarlenEntry>(
          schema, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2));
    } else {
      Util::SetPRAttribute<storage::VarlenEntry>(
          schema, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2));
    }

    // C_CREDIT_LIM = 50,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_CREDIT_LIM", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 50000.0);

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_DISCOUNT", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr,
                                 Util::RandomWithin<double>(0, 5000, 4, generator));

    // C_BALANCE = -10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_BALANCE", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, -10.0);

    // C_YTD_PAYMENT = 10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_YTD_PAYMENT", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 10.0);

    // C_PAYMENT_CNT = 1
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_PAYMENT_CNT", "Wrong attribute.");
    Util::SetPRAttribute<int16_t>(schema, col_offset++, projection_map, pr, 1);

    // C_DELIVERY_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_DELIVERY_CNT", "Wrong attribute.");
    Util::SetPRAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // C_DATA random a-string [300 .. 500]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "C_DATA", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(300, 500, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Customer tuple.");

    return pr;
  }

  template <class Random>
  static storage::ProjectedRow *BuildHistoryTuple(const int32_t c_id, const int32_t d_id, const int32_t w_id,
                                                  byte *const buffer,
                                                  const storage::ProjectedRowInitializer &pr_initializer,
                                                  const storage::ProjectionMap &projection_map,
                                                  const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // H_C_ID = C_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_C_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // H_C_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_C_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // H_C_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_C_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // H_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // H_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // H_DATE current date and time
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_DATE", "Wrong attribute.");
    Util::SetPRAttribute<uint64_t>(schema, col_offset++, projection_map, pr, Util::Timestamp());

    // H_AMOUNT = 10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_AMOUNT", "Wrong attribute.");
    Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 10.0);

    // H_DATA random a-string [12 .. 24]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "H_DATA", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(12, 24, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for History tuple.");

    return pr;
  }

  static storage::ProjectedRow *BuildNewOrderTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                                   byte *const buffer,
                                                   const storage::ProjectedRowInitializer &pr_initializer,
                                                   const storage::ProjectionMap &projection_map,
                                                   const catalog::Schema &schema) {
    TERRIER_ASSERT(o_id >= 2101 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // NO_O_ID = O_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "NO_O_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // NO_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "NO_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // NO_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "NO_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for New Order tuple.");

    return pr;
  }

  struct OrderTupleResults {
    storage::ProjectedRow *const pr;
    uint64_t o_entry_d;
    int8_t o_ol_cnt;
  };

  template <class Random>
  static OrderTupleResults BuildOrderTuple(const int32_t o_id, const int32_t c_id, const int32_t d_id,
                                           const int32_t w_id, byte *const buffer,
                                           const storage::ProjectedRowInitializer &pr_initializer,
                                           const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                                           Random *const generator) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(c_id >= 1 && c_id <= num_customers_per_district_, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // O_ID unique within [3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // O_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // O_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // O_C_ID selected sequentially from a random permutation of [1 .. 3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_C_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // O_ENTRY_D current date/ time given by the operating system
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_ENTRY_D", "Wrong attribute.");
    const uint64_t entry_d = Util::Timestamp();
    Util::SetPRAttribute<uint64_t>(schema, col_offset++, projection_map, pr, entry_d);

    // O_CARRIER_ID random within [1 .. 10] if O_ID < 2,101, null otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_CARRIER_ID", "Wrong attribute.");
    const auto col_oid = schema.GetColumn(col_offset++).GetOid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<int32_t *>(attr) = Util::RandomWithin<int32_t>(1, 10, 0, generator);
    } else {
      pr->SetNull(attr_offset);
    }

    // O_OL_CNT random within [5 .. 15]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_OL_CNT", "Wrong attribute.");
    const auto ol_cnt = Util::RandomWithin<int32_t>(5, 15, 0, generator);
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, ol_cnt);

    // O_ALL_LOCAL = 1
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "O_ALL_LOCAL", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, 1);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order tuple.");

    return {pr, entry_d, static_cast<int8_t>(ol_cnt)};
  }

  template <class Random>
  static storage::ProjectedRow *BuildOrderLineTuple(const int32_t o_id, const int32_t d_id, const int32_t w_id,
                                                    const int32_t ol_number, const uint64_t o_entry_d,
                                                    byte *const buffer,
                                                    const storage::ProjectedRowInitializer &pr_initializer,
                                                    const storage::ProjectionMap &projection_map,
                                                    const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= num_districts_per_warehouse_, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // OL_O_ID = O_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_O_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // OL_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_D_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, d_id);

    // OL_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // OL_NUMBER unique within [O_OL_CNT]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_NUMBER", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, ol_number);

    // OL_I_ID random within [1 .. 100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_I_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr,
                                  Util::RandomWithin<int32_t>(1, 100000, 0, generator));

    // OL_SUPPLY_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_SUPPLY_W_ID", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, w_id);

    // OL_DELIVERY_D = O_ENTRY_D if OL_O_ID < 2,101, null otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_DELIVERY_D", "Wrong attribute.");
    const auto col_oid = schema.GetColumn(col_offset++).GetOid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<uint64_t *>(attr) = o_entry_d;
    } else {
      pr->SetNull(attr_offset);
    }

    // OL_QUANTITY = 5
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_QUANTITY", "Wrong attribute.");
    Util::SetPRAttribute<int32_t>(schema, col_offset++, projection_map, pr, 5);

    // OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_AMOUNT", "Wrong attribute.");
    if (o_id < 2101) {
      Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr, 0.0);
    } else {
      Util::SetPRAttribute<double>(schema, col_offset++, projection_map, pr,
                                   Util::RandomWithin<double>(1, 999999, 2, generator));
    }

    // OL_DIST_INFO random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).GetName() == "OL_DIST_INFO", "Wrong attribute.");
    Util::SetPRAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                               Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order Line tuple.");

    return pr;
  }
};

}  // namespace terrier::tpcc