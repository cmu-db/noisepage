#pragma once

#include <unordered_map>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/index/index.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/util.h"
#include "test_util/tpcc/worker.h"
#include "transaction/transaction_manager.h"

namespace terrier::tpcc {

/**
 * Loads the TPC-C tables and indexes according to section 4.3.3.1 of the specification
 */
struct Loader {
  Loader() = delete;

  static void PopulateDatabase(common::ManagedPointer<transaction::TransactionManager> txn_manager, Database *const db,
                               std::vector<Worker> *const workers, common::WorkerPool *const thread_pool) {
    TERRIER_ASSERT(txn_manager != nullptr, "TransactionManager does not exist.");
    TERRIER_ASSERT(db != nullptr, "Database does not exist.");

    const auto num_warehouses = static_cast<int8_t>(workers->size());

    // Item tuple
    const auto item_tuple_col_oids = Util::AllColOidsForSchema(db->item_schema_);

    const auto item_tuple_pr_initializer = db->item_table_->InitializerForProjectedRow(item_tuple_col_oids);
    const auto item_tuple_pr_map = db->item_table_->ProjectionMapForOids(item_tuple_col_oids);

    // Item key
    const auto item_key_pr_initializer = db->item_primary_index_->GetProjectedRowInitializer();
    const auto item_key_pr_map = db->item_primary_index_->GetKeyOidToOffsetMap();

    // Warehouse tuple
    const auto warehouse_tuple_col_oids = Util::AllColOidsForSchema(db->warehouse_schema_);

    const auto warehouse_tuple_pr_initializer =
        db->warehouse_table_->InitializerForProjectedRow(warehouse_tuple_col_oids);
    const auto warehouse_tuple_pr_map = db->warehouse_table_->ProjectionMapForOids(warehouse_tuple_col_oids);

    // Warehouse key
    const auto warehouse_key_pr_initializer = db->warehouse_primary_index_->GetProjectedRowInitializer();
    const auto warehouse_key_pr_map = db->warehouse_primary_index_->GetKeyOidToOffsetMap();

    // Stock tuple
    const auto stock_tuple_col_oids = Util::AllColOidsForSchema(db->stock_schema_);

    const auto stock_tuple_pr_initializer = db->stock_table_->InitializerForProjectedRow(stock_tuple_col_oids);
    const auto stock_tuple_pr_map = db->stock_table_->ProjectionMapForOids(stock_tuple_col_oids);

    // Stock key
    const auto stock_key_pr_initializer = db->stock_primary_index_->GetProjectedRowInitializer();
    const auto stock_key_pr_map = db->stock_primary_index_->GetKeyOidToOffsetMap();

    // District tuple
    const auto district_tuple_col_oids = Util::AllColOidsForSchema(db->district_schema_);

    const auto district_tuple_pr_initializer = db->district_table_->InitializerForProjectedRow(district_tuple_col_oids);
    const auto district_tuple_pr_map = db->district_table_->ProjectionMapForOids(district_tuple_col_oids);

    // District key
    const auto district_key_pr_initializer = db->district_primary_index_->GetProjectedRowInitializer();
    const auto district_key_pr_map = db->district_primary_index_->GetKeyOidToOffsetMap();

    // Customer tuple
    const auto customer_tuple_col_oids = Util::AllColOidsForSchema(db->customer_schema_);

    const auto customer_tuple_pr_initializer = db->customer_table_->InitializerForProjectedRow(customer_tuple_col_oids);
    const auto customer_tuple_pr_map = db->customer_table_->ProjectionMapForOids(customer_tuple_col_oids);

    // Customer key
    const auto customer_key_pr_initializer = db->customer_primary_index_->GetProjectedRowInitializer();
    const auto customer_key_pr_map = db->customer_primary_index_->GetKeyOidToOffsetMap();

    // Customer Name key
    const auto customer_name_key_pr_initializer = db->customer_secondary_index_->GetProjectedRowInitializer();
    const auto customer_name_key_pr_map = db->customer_secondary_index_->GetKeyOidToOffsetMap();

    // History tuple
    const auto history_tuple_col_oids = Util::AllColOidsForSchema(db->history_schema_);

    const auto history_tuple_pr_initializer = db->history_table_->InitializerForProjectedRow(history_tuple_col_oids);
    const auto history_tuple_pr_map = db->history_table_->ProjectionMapForOids(history_tuple_col_oids);

    // Order tuple
    const auto order_tuple_col_oids = Util::AllColOidsForSchema(db->order_schema_);

    const auto order_tuple_pr_initializer = db->order_table_->InitializerForProjectedRow(order_tuple_col_oids);
    const auto order_tuple_pr_map = db->order_table_->ProjectionMapForOids(order_tuple_col_oids);

    // Order key
    const auto order_key_pr_initializer = db->order_primary_index_->GetProjectedRowInitializer();
    const auto order_key_pr_map = db->order_primary_index_->GetKeyOidToOffsetMap();

    // Order secondary key
    const auto order_secondary_key_pr_initializer = db->order_secondary_index_->GetProjectedRowInitializer();
    const auto order_secondary_key_pr_map = db->order_secondary_index_->GetKeyOidToOffsetMap();

    // New Order tuple
    const auto new_order_tuple_col_oids = Util::AllColOidsForSchema(db->new_order_schema_);

    const auto new_order_tuple_pr_initializer =
        db->new_order_table_->InitializerForProjectedRow(new_order_tuple_col_oids);
    const auto new_order_tuple_pr_map = db->new_order_table_->ProjectionMapForOids(new_order_tuple_col_oids);

    // New Order key
    const auto new_order_key_pr_initializer = db->new_order_primary_index_->GetProjectedRowInitializer();
    const auto new_order_key_pr_map = db->new_order_primary_index_->GetKeyOidToOffsetMap();

    // Order Line tuple
    const auto order_line_tuple_col_oids = Util::AllColOidsForSchema(db->order_line_schema_);

    const auto order_line_tuple_pr_initializer =
        db->order_line_table_->InitializerForProjectedRow(order_line_tuple_col_oids);
    const auto order_line_tuple_pr_map = db->order_line_table_->ProjectionMapForOids(order_line_tuple_col_oids);

    // Order Line key
    const auto order_line_key_pr_initializer = db->order_line_primary_index_->GetProjectedRowInitializer();
    const auto order_line_key_pr_map = db->order_line_primary_index_->GetKeyOidToOffsetMap();

    // Populate the ITEM table. We don't do this in parallel since it's not very big. Just use the first Worker for it.
    const auto *worker = &(workers->front());
    auto *const item_txn = txn_manager->BeginTransaction();
    {
      // generate booleans to represent ORIGINAL for item. 10% are ORIGINAL (true), and then shuffled
      std::vector<bool> item_original;
      item_original.reserve(100000);
      for (uint32_t i_id = 0; i_id < 100000; i_id++) {
        item_original.emplace_back(i_id < 10000);
      }
      std::shuffle(item_original.begin(), item_original.end(), *(worker->generator_));

      for (int32_t i_id = 0; i_id < 100000; i_id++) {
        // 100,000 rows in the ITEM table
        // insert in table
        auto *const item_redo = item_txn->StageWrite(db->db_oid_, db->item_table_oid_, item_tuple_pr_initializer);
        BuildItemTuple(i_id + 1, item_original[i_id], item_redo->Delta(), item_tuple_pr_map, db->item_schema_,
                       worker->generator_);
        const auto item_slot = db->item_table_->Insert(common::ManagedPointer(item_txn), item_redo);

        // insert in index
        const auto *const item_key = BuildItemKey(i_id + 1, worker->item_key_buffer_, item_key_pr_initializer,
                                                  item_key_pr_map, db->item_primary_index_schema_);
        bool UNUSED_ATTRIBUTE index_insert_result =
            db->item_primary_index_->InsertUnique(common::ManagedPointer(item_txn), *item_key, item_slot);
        TERRIER_ASSERT(index_insert_result, "Item index insertion failed.");
      }
    }
    txn_manager->Commit(item_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Populate the other tables in parallel
    for (int8_t w_id = 0; w_id < num_warehouses; w_id++) {
      // copy the pr_map and pr_initializers by reference since they're on the stack in this scope, but the remaining
      // are pointers or integers so we'll copy by value
      thread_pool->SubmitTask([&, w_id, txn_manager, db] {
        Worker *const worker = &((*workers)[w_id]);

        auto *const txn = txn_manager->BeginTransaction();

        // 1 row in the WAREHOUSE table for each configured warehouse
        // insert in table
        auto *const warehouse_redo =
            txn->StageWrite(db->db_oid_, db->warehouse_table_oid_, warehouse_tuple_pr_initializer);
        BuildWarehouseTuple(static_cast<int8_t>(w_id + 1), warehouse_redo->Delta(), warehouse_tuple_pr_map,
                            db->warehouse_schema_, worker->generator_);
        const auto warehouse_slot = db->warehouse_table_->Insert(common::ManagedPointer(txn), warehouse_redo);

        // insert in index
        const auto *const warehouse_key =
            BuildWarehouseKey(static_cast<int8_t>(w_id + 1), worker->warehouse_key_buffer_,
                              warehouse_key_pr_initializer, warehouse_key_pr_map, db->warehouse_primary_index_schema_);
        bool UNUSED_ATTRIBUTE index_insert_result =
            db->warehouse_primary_index_->InsertUnique(common::ManagedPointer(txn), *warehouse_key, warehouse_slot);
        TERRIER_ASSERT(index_insert_result, "Warehouse index insertion failed.");

        {
          // generate booleans to represent ORIGINAL for stock. 10% are ORIGINAL (true), and then shuffled
          std::vector<bool> stock_original;
          stock_original.reserve(100000);
          for (int32_t i_id = 0; i_id < 100000; i_id++) {
            stock_original.emplace_back(i_id < 10000);
          }
          std::shuffle(stock_original.begin(), stock_original.end(), *(worker->generator_));

          for (int32_t s_i_id = 0; s_i_id < 100000; s_i_id++) {
            // For each row in the WAREHOUSE table:
            // 100,000 rows in the STOCK table

            // insert in table
            auto *const stock_redo = txn->StageWrite(db->db_oid_, db->stock_table_oid_, stock_tuple_pr_initializer);
            BuildStockTuple(s_i_id + 1, static_cast<int8_t>(w_id + 1), stock_original[s_i_id], stock_redo->Delta(),
                            stock_tuple_pr_map, db->stock_schema_, worker->generator_);
            const auto stock_slot = db->stock_table_->Insert(common::ManagedPointer(txn), stock_redo);

            // insert in index
            const auto *const stock_key =
                BuildStockKey(s_i_id + 1, static_cast<int8_t>(w_id + 1), worker->stock_key_buffer_,
                              stock_key_pr_initializer, stock_key_pr_map, db->stock_primary_index_schema_);
            index_insert_result =
                db->stock_primary_index_->InsertUnique(common::ManagedPointer(txn), *stock_key, stock_slot);
            TERRIER_ASSERT(index_insert_result, "Stock index insertion failed.");
          }
        }

        for (int8_t d_id = 0; d_id < 10; d_id++) {
          // For each row in the WAREHOUSE table:
          // 10 rows in the DISTRICT table

          // insert in table
          auto *const district_redo =
              txn->StageWrite(db->db_oid_, db->district_table_oid_, district_tuple_pr_initializer);
          BuildDistrictTuple(static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), district_redo->Delta(),
                             district_tuple_pr_map, db->district_schema_, worker->generator_);
          const auto district_slot = db->district_table_->Insert(common::ManagedPointer(txn), district_redo);

          // insert in index
          const auto *const district_key = BuildDistrictKey(
              static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), worker->district_key_buffer_,
              district_key_pr_initializer, district_key_pr_map, db->district_primary_index_schema_);
          index_insert_result =
              db->district_primary_index_->InsertUnique(common::ManagedPointer(txn), *district_key, district_slot);
          TERRIER_ASSERT(index_insert_result, "District index insertion failed.");

          // O_C_ID selected sequentially from a random permutation of [1 .. 3,000] for Order table
          std::vector<int32_t> o_c_ids;
          o_c_ids.reserve(3000);
          // generate booleans to represent GC or BC for customers. 90% are GC (true), and then shuffled
          std::vector<bool> c_credit;
          c_credit.reserve(3000);
          for (int32_t c_id = 0; c_id < 3000; c_id++) {
            c_credit.emplace_back(c_id < 3000 / 10);
            o_c_ids.emplace_back(c_id + 1);
          }
          std::shuffle(c_credit.begin(), c_credit.end(), *(worker->generator_));
          std::shuffle(o_c_ids.begin(), o_c_ids.end(), *(worker->generator_));

          for (int32_t c_id = 0; c_id < 3000; c_id++) {
            // For each row in the DISTRICT table:
            // 3,000 rows in the CUSTOMER table

            // insert in table
            auto *const customer_redo =
                txn->StageWrite(db->db_oid_, db->customer_table_oid_, customer_tuple_pr_initializer);
            BuildCustomerTuple(c_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), c_credit[c_id],
                               customer_redo->Delta(), customer_tuple_pr_map, db->customer_schema_, worker->generator_);
            const auto customer_slot = db->customer_table_->Insert(common::ManagedPointer(txn), customer_redo);

            // insert in index
            const auto *const customer_key = BuildCustomerKey(
                c_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), worker->customer_key_buffer_,
                customer_key_pr_initializer, customer_key_pr_map, db->customer_primary_index_schema_);
            index_insert_result =
                db->customer_primary_index_->InsertUnique(common::ManagedPointer(txn), *customer_key, customer_slot);
            TERRIER_ASSERT(index_insert_result, "Customer index insertion failed.");

            // insert in customer name index
            const auto c_last_tuple =
                *reinterpret_cast<const storage::VarlenEntry *const>(customer_redo->Delta()->AccessWithNullCheck(
                    customer_tuple_pr_map.at(db->customer_schema_.GetColumn(5).Oid())));

            storage::ProjectedRow *customer_name_key = nullptr;
            if (c_last_tuple.Size() <= storage::VarlenEntry::InlineThreshold()) {
              customer_name_key =
                  BuildCustomerNameKey(c_last_tuple, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                                       worker->customer_name_key_buffer_, customer_name_key_pr_initializer,
                                       customer_name_key_pr_map, db->customer_secondary_index_schema_);
            } else {
              std::memcpy(worker->customer_name_varlen_buffer_, c_last_tuple.Content(), c_last_tuple.Size());
              const auto c_last_key =
                  storage::VarlenEntry::Create(worker->customer_name_varlen_buffer_, c_last_tuple.Size(), false);

              customer_name_key =
                  BuildCustomerNameKey(c_last_key, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                                       worker->customer_name_key_buffer_, customer_name_key_pr_initializer,
                                       customer_name_key_pr_map, db->customer_secondary_index_schema_);
            }

            index_insert_result =
                db->customer_secondary_index_->Insert(common::ManagedPointer(txn), *customer_name_key, customer_slot);
            TERRIER_ASSERT(index_insert_result, "Customer Name index insertion failed.");

            // For each row in the CUSTOMER table:
            // 1 row in the HISTORY table
            auto *const history_redo =
                txn->StageWrite(db->db_oid_, db->history_table_oid_, history_tuple_pr_initializer);
            BuildHistoryTuple(c_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                              history_redo->Delta(), history_tuple_pr_map, db->history_schema_, worker->generator_);
            db->history_table_->Insert(common::ManagedPointer(txn), history_redo);

            // For each row in the DISTRICT table:
            // 3,000 rows in the ORDER table

            // insert in table
            const auto o_id = c_id;
            auto *const order_redo = txn->StageWrite(db->db_oid_, db->order_table_oid_, order_tuple_pr_initializer);
            const auto order_results =
                BuildOrderTuple(o_id + 1, o_c_ids[c_id], static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                                order_redo->Delta(), order_tuple_pr_map, db->order_schema_, worker->generator_);
            const auto order_slot = db->order_table_->Insert(common::ManagedPointer(txn), order_redo);

            // insert in index
            const auto *const order_key = BuildOrderKey(
                o_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), worker->order_key_buffer_,
                order_key_pr_initializer, order_key_pr_map, db->order_primary_index_schema_);
            index_insert_result =
                db->order_primary_index_->InsertUnique(common::ManagedPointer(txn), *order_key, order_slot);
            TERRIER_ASSERT(index_insert_result, "Order index insertion failed.");

            // insert in secondary index
            const auto *const order_secondary_key = BuildOrderSecondaryKey(
                o_id + 1, o_c_ids[c_id], static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                worker->order_secondary_key_buffer_, order_secondary_key_pr_initializer, order_secondary_key_pr_map,
                db->order_secondary_index_schema_);
            index_insert_result =
                db->order_secondary_index_->InsertUnique(common::ManagedPointer(txn), *order_secondary_key, order_slot);
            TERRIER_ASSERT(index_insert_result, "Order secondary index insertion failed.");

            // For each row in the ORDER table:
            // A number of rows in the ORDER-LINE table equal to O_OL_CNT, generated according to the rules for input
            // data generation of the New-Order transaction (see Clause 2.4.1)
            for (int8_t ol_number = 0; ol_number < order_results.o_ol_cnt_; ol_number++) {
              // insert in table
              auto *const order_line_redo =
                  txn->StageWrite(db->db_oid_, db->order_line_table_oid_, order_line_tuple_pr_initializer);
              BuildOrderLineTuple(o_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                                  static_cast<int8_t>(ol_number + 1), order_results.o_entry_d_,
                                  order_line_redo->Delta(), order_line_tuple_pr_map, db->order_line_schema_,
                                  worker->generator_);
              const auto order_line_slot = db->order_line_table_->Insert(common::ManagedPointer(txn), order_line_redo);

              // insert in index
              const auto *const order_line_key = BuildOrderLineKey(
                  o_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                  static_cast<int8_t>(ol_number + 1), worker->order_line_key_buffer_, order_line_key_pr_initializer,
                  order_line_key_pr_map, db->order_line_primary_index_schema_);
              index_insert_result = db->order_line_primary_index_->InsertUnique(common::ManagedPointer(txn),
                                                                                *order_line_key, order_line_slot);
              TERRIER_ASSERT(index_insert_result, "Order Line index insertion failed.");
            }

            // For each row in the DISTRICT table:
            // 900 rows in the NEW-ORDER table corresponding to the last 900 rows in the ORDER table for that district
            // (i.e., with NO_O_ID between 2,101 and 3,000)
            if (o_id + 1 >= 2101) {
              // insert in table
              auto *const new_order_redo =
                  txn->StageWrite(db->db_oid_, db->new_order_table_oid_, new_order_tuple_pr_initializer);
              BuildNewOrderTuple(o_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1),
                                 new_order_redo->Delta(), new_order_tuple_pr_map, db->new_order_schema_);
              const auto new_order_slot = db->new_order_table_->Insert(common::ManagedPointer(txn), new_order_redo);

              // insert in index
              const auto *const new_order_key = BuildNewOrderKey(
                  o_id + 1, static_cast<int8_t>(d_id + 1), static_cast<int8_t>(w_id + 1), worker->new_order_key_buffer_,
                  new_order_key_pr_initializer, new_order_key_pr_map, db->new_order_primary_index_schema_);
              index_insert_result = db->new_order_primary_index_->InsertUnique(common::ManagedPointer(txn),
                                                                               *new_order_key, new_order_slot);
              TERRIER_ASSERT(index_insert_result, "New Order index insertion failed.");
            }
          }
        }

        txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      });
    }
    thread_pool->WaitUntilAllFinished();
  }

  template <class Random>
  static void BuildItemTuple(const int32_t i_id, const bool original, storage::ProjectedRow *const pr,
                             const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                             Random *const generator) {
    TERRIER_ASSERT(i_id >= 1 && i_id <= 100000, "Invalid i_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // I_ID unique within [100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_id", "Wrong attribute.");
    Util::Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, i_id);

    // I_IM_ID random within [1 .. 10,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_im_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr,
                                     Util::RandomWithin<int32_t>(1, 10000, 0, generator));

    // I_NAME random a-string [14 .. 24]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_name", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(14, 24, false, generator));

    // I_PRICE random within [1.00 .. 100.00]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_price", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr,
                                    Util::RandomWithin<double>(100, 10000, 2, generator));

    // I_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within I_DATA
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_data", "Wrong attribute.");
    if (original) {
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::OriginalVarlenEntry(26, 50, generator));
    } else {
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::AlphaNumericVarlenEntry(26, 50, false, generator));
    }

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Item tuple.");
  }

  static storage::ProjectedRow *BuildItemKey(const int32_t i_id, byte *const buffer,
                                             const storage::ProjectedRowInitializer &pr_initializer,
                                             const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                             const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(i_id >= 1 && i_id <= 100000, "Invalid i_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: I_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "i_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, i_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Item key.");

    return pr;
  }

  template <class Random>
  static void BuildWarehouseTuple(const int8_t w_id, storage::ProjectedRow *const pr,
                                  const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                                  Random *const generator) {
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // W_ID unique within [number_of_configured_warehouses]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_id", "Wrong attribute.");
    Util::SetTupleAttribute(schema, col_offset++, projection_map, pr, w_id);

    // W_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_name", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(6, 10, false, generator));

    // W_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_street_1", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_street_2", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_city", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // W_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_state", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // W_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_zip", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::ZipVarlenEntry(generator));

    // W_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_tax", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr,
                                    Util::RandomWithin<double>(0, 2000, 4, generator));

    // W_YTD = 300,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_ytd", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 300000.0);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Warehouse tuple.");
  }

  static storage::ProjectedRow *BuildWarehouseKey(
      const int8_t w_id, byte *const buffer, const storage::ProjectedRowInitializer &pr_initializer,
      const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map, const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Warehouse key.");

    return pr;
  }

  template <class Random>
  static void BuildStockTuple(const int32_t s_i_id, const int8_t w_id, const bool original,

                              storage::ProjectedRow *const pr, const storage::ProjectionMap &projection_map,
                              const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(s_i_id >= 1 && s_i_id <= 100000, "Invalid s_i_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // S_I_ID unique within [100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_i_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, s_i_id);

    // S_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // S_QUANTITY random within [10 .. 100]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_quantity", "Wrong attribute.");
    Util::SetTupleAttribute<int16_t>(schema, col_offset++, projection_map, pr,
                                     Util::RandomWithin<int16_t>(10, 100, 0, generator));

    // S_DIST_01 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_01", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_02 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_02", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_03 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_03", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_04 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_04", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_05 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_05", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_06 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_06", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_07 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_07", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_08 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_08", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_09 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_09", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_DIST_10 random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_dist_10", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    // S_YTD = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_ytd", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, 0);

    // S_ORDER_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_order_cnt", "Wrong attribute.");
    Util::SetTupleAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // S_REMOTE_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_remote_cnt", "Wrong attribute.");
    Util::SetTupleAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // S_DATA random a-string [26 .. 50]. For 10% of the rows, selected at random, the string "ORIGINAL" must be held by
    // 8 consecutive characters starting at a random position within S_DATA
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_data", "Wrong attribute.");
    if (original) {
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::OriginalVarlenEntry(26, 50, generator));
    } else {
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::AlphaNumericVarlenEntry(26, 50, false, generator));
    }

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Stock tuple.");
  }

  static storage::ProjectedRow *BuildStockKey(const int32_t s_i_id, const int8_t w_id, byte *const buffer,
                                              const storage::ProjectedRowInitializer &pr_initializer,
                                              const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                              const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(s_i_id >= 1 && s_i_id <= 100000, "Invalid s_i_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (S_W_ID, S_I_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "s_i_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, s_i_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Stock key.");

    return pr;
  }

  template <class Random>
  static void BuildDistrictTuple(const int8_t d_id, const int8_t w_id, storage::ProjectedRow *const pr,
                                 const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                                 Random *const generator) {
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // D_ID unique within [10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // D_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // D_NAME random a-string [6 .. 10]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_name", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(6, 10, false, generator));

    // D_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_street_1", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_street_2", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_city", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // D_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_state", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // D_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_zip", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::ZipVarlenEntry(generator));

    // D_TAX random within [0.0000 .. 0.2000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_tax", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr,
                                    Util::RandomWithin<double>(0, 2000, 4, generator));

    // D_YTD = 30,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_ytd", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 30000.0);

    // D_NEXT_O_ID = 3,001
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_next_o_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, 3001);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for District tuple.");
  }

  static storage::ProjectedRow *BuildDistrictKey(const int8_t d_id, const int8_t w_id, byte *const buffer,
                                                 const storage::ProjectedRowInitializer &pr_initializer,
                                                 const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                                 const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (D_W_ID, D_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for District key.");

    return pr;
  }

  template <class Random>
  static void BuildCustomerTuple(const int32_t c_id, const int8_t d_id, const int8_t w_id, const bool good_credit,
                                 storage::ProjectedRow *const pr, const storage::ProjectionMap &projection_map,
                                 const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // C_ID unique within [3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // C_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // C_W_ID = D_W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // C_FIRST random a-string [8 .. 16]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_first", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(8, 16, false, generator));

    // C_MIDDLE = "OE"
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_middle", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(
        schema, col_offset++, projection_map, pr,
        storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("OE"), 2));

    // C_LAST generated according to Clause 4.3.2.3, iterating through the range of [0 .. 999] for the first 1,000
    // customers, and generating a non-uniform random number using the function NURand(255,0,999) for each of the
    // remaining 2,000 customers. The run-time constant C (see Clause 2.1.6) used for the database population must be
    // randomly chosen independently from the test run(s).
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_last", "Wrong attribute.");
    if (c_id <= 1000) {
      const auto rand_num = static_cast<const uint16_t>(c_id - 1);
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::LastNameVarlenEntry(rand_num));
    } else {
      const auto rand_num = static_cast<const uint16_t>(Util::NURand(255, 0, 999, generator));
      Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                    Util::LastNameVarlenEntry(rand_num));
    }

    // C_STREET_1 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_street_1", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_STREET_2 random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_street_2", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_CITY random a-string [10 .. 20]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_city", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(10, 20, false, generator));

    // C_STATE random a-string of 2 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_state", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(2, 2, false, generator));

    // C_ZIP generated according to Clause 4.3.2.7
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_zip", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::ZipVarlenEntry(generator));

    // C_PHONE random n-string of 16 numbers
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_phone", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(16, 16, true, generator));

    // C_SINCE date/ time given by the operating system when the CUSTOMER table was populated.
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_since", "Wrong attribute.");
    Util::SetTupleAttribute<uint64_t>(schema, col_offset++, projection_map, pr, Util::Timestamp());

    // C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_credit", "Wrong attribute.");
    if (good_credit) {
      Util::SetTupleAttribute<storage::VarlenEntry>(
          schema, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("GC"), 2));
    } else {
      Util::SetTupleAttribute<storage::VarlenEntry>(
          schema, col_offset++, projection_map, pr,
          storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>("BC"), 2));
    }

    // C_CREDIT_LIM = 50,000.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_credit_lim", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 50000.0);

    // C_DISCOUNT random within [0.0000 .. 0.5000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_discount", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr,
                                    Util::RandomWithin<double>(0, 5000, 4, generator));

    // C_BALANCE = -10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_balance", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, -10.0);

    // C_YTD_PAYMENT = 10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_ytd_payment", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 10.0);

    // C_PAYMENT_CNT = 1
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_payment_cnt", "Wrong attribute.");
    Util::SetTupleAttribute<int16_t>(schema, col_offset++, projection_map, pr, 1);

    // C_DELIVERY_CNT = 0
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_delivery_cnt", "Wrong attribute.");
    Util::SetTupleAttribute<int16_t>(schema, col_offset++, projection_map, pr, 0);

    // C_DATA random a-string [300 .. 500]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_data", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(300, 500, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Customer tuple.");
  }

  static storage::ProjectedRow *BuildCustomerKey(const int32_t c_id, const int8_t d_id, const int8_t w_id,
                                                 byte *const buffer,
                                                 const storage::ProjectedRowInitializer &pr_initializer,
                                                 const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                                 const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (C_W_ID, C_D_ID, C_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, c_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Customer key.");

    return pr;
  }

  static storage::ProjectedRow *BuildCustomerNameKey(
      const storage::VarlenEntry &c_last, const int8_t d_id, const int8_t w_id, byte *const buffer,
      const storage::ProjectedRowInitializer &pr_initializer,
      const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map, const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (C_W_ID, C_D_ID, C_LAST)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "c_last", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, c_last);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Customer key.");

    return pr;
  }

  template <class Random>
  static void BuildHistoryTuple(const int32_t c_id, const int8_t d_id, const int8_t w_id,
                                storage::ProjectedRow *const pr, const storage::ProjectionMap &projection_map,
                                const catalog::Schema &schema, Random *const generator) {
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // H_C_ID = C_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_c_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // H_C_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_c_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // H_C_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_c_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // H_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // H_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // H_DATE current date and time
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_date", "Wrong attribute.");
    Util::SetTupleAttribute<uint64_t>(schema, col_offset++, projection_map, pr, Util::Timestamp());

    // H_AMOUNT = 10.00
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_amount", "Wrong attribute.");
    Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 10.0);

    // H_DATA random a-string [12 .. 24]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "h_data", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(12, 24, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for History tuple.");
  }

  static void BuildNewOrderTuple(const int32_t o_id, const int8_t d_id, const int8_t w_id,
                                 storage::ProjectedRow *const pr, const storage::ProjectionMap &projection_map,
                                 const catalog::Schema &schema) {
    TERRIER_ASSERT(o_id >= 2101 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // NO_O_ID = O_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_o_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // NO_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // NO_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for New Order tuple.");
  }

  static storage::ProjectedRow *BuildNewOrderKey(const int32_t o_id, const int8_t d_id, const int8_t w_id,
                                                 byte *const buffer,
                                                 const storage::ProjectedRowInitializer &pr_initializer,
                                                 const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                                 const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(o_id >= 2101 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (NO_W_ID, NO_D_ID, NO_O_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "no_o_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, o_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for New Order key.");

    return pr;
  }

  struct OrderTupleResults {
    uint64_t o_entry_d_;
    int8_t o_ol_cnt_;
  };

  template <class Random>
  static OrderTupleResults BuildOrderTuple(const int32_t o_id, const int32_t c_id, const int8_t d_id, const int8_t w_id,
                                           storage::ProjectedRow *const pr,
                                           const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                                           Random *const generator) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // O_ID unique within [3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // O_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // O_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // O_C_ID selected sequentially from a random permutation of [1 .. 3,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_c_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, c_id);

    // O_ENTRY_D current date/ time given by the operating system
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_entry_d", "Wrong attribute.");
    const uint64_t entry_d = Util::Timestamp();
    Util::SetTupleAttribute<uint64_t>(schema, col_offset++, projection_map, pr, entry_d);

    // O_CARRIER_ID random within [1 .. 10] if O_ID < 2,101, null otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_carrier_id", "Wrong attribute.");
    const auto col_oid = schema.GetColumn(col_offset++).Oid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<int8_t *>(attr) = Util::RandomWithin<int8_t>(1, 10, 0, generator);
    } else {
      pr->SetNull(attr_offset);
    }

    // O_OL_CNT random within [5 .. 15]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_ol_cnt", "Wrong attribute.");
    const auto ol_cnt = Util::RandomWithin<int8_t>(5, 15, 0, generator);
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, ol_cnt);

    // O_ALL_LOCAL = 1
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_all_local", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, 1);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order tuple.");

    return {entry_d, static_cast<int8_t>(ol_cnt)};
  }

  static storage::ProjectedRow *BuildOrderKey(const int32_t o_id, const int8_t d_id, const int8_t w_id,
                                              byte *const buffer,
                                              const storage::ProjectedRowInitializer &pr_initializer,
                                              const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map,
                                              const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (O_W_ID, O_D_ID, O_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, o_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order key.");

    return pr;
  }

  static storage::ProjectedRow *BuildOrderSecondaryKey(
      const int32_t o_id, const int32_t c_id, const int8_t d_id, const int8_t w_id, byte *const buffer,
      const storage::ProjectedRowInitializer &pr_initializer,
      const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map, const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Secondary Key: (O_W_ID, O_D_ID, O_C_ID O_ID)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_c_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, c_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "o_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, o_id);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order secondary key.");

    return pr;
  }

  template <class Random>
  static void BuildOrderLineTuple(const int32_t o_id, const int8_t d_id, const int8_t w_id, const int8_t ol_number,
                                  const uint64_t o_entry_d, storage::ProjectedRow *const pr,
                                  const storage::ProjectionMap &projection_map, const catalog::Schema &schema,
                                  Random *const generator) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(pr != nullptr, "ProjectedRow is nullptr.");

    uint32_t col_offset = 0;

    // OL_O_ID = O_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_o_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr, o_id);

    // OL_D_ID = D_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_d_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, d_id);

    // OL_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // OL_NUMBER unique within [O_OL_CNT]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_number", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, ol_number);

    // OL_I_ID random within [1 .. 100,000]
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_i_id", "Wrong attribute.");
    Util::SetTupleAttribute<int32_t>(schema, col_offset++, projection_map, pr,
                                     Util::RandomWithin<int32_t>(1, 100000, 0, generator));

    // OL_SUPPLY_W_ID = W_ID
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_supply_w_id", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, w_id);

    // OL_DELIVERY_D = O_ENTRY_D if OL_O_ID < 2,101, null otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_delivery_d", "Wrong attribute.");
    const auto col_oid = schema.GetColumn(col_offset++).Oid();
    const auto attr_offset = projection_map.at(col_oid);
    if (o_id < 2101) {
      auto *const attr = pr->AccessForceNotNull(attr_offset);
      *reinterpret_cast<uint64_t *>(attr) = o_entry_d;
    } else {
      pr->SetNull(attr_offset);
    }

    // OL_QUANTITY = 5
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_quantity", "Wrong attribute.");
    Util::SetTupleAttribute<int8_t>(schema, col_offset++, projection_map, pr, 5);

    // OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_amount", "Wrong attribute.");
    if (o_id < 2101) {
      Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr, 0.0);
    } else {
      Util::SetTupleAttribute<double>(schema, col_offset++, projection_map, pr,
                                      Util::RandomWithin<double>(1, 999999, 2, generator));
    }

    // OL_DIST_INFO random a-string of 24 letters
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_dist_info", "Wrong attribute.");
    Util::SetTupleAttribute<storage::VarlenEntry>(schema, col_offset++, projection_map, pr,
                                                  Util::AlphaNumericVarlenEntry(24, 24, false, generator));

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order Line tuple.");
  }

  static storage::ProjectedRow *BuildOrderLineKey(
      const int32_t o_id, const int8_t d_id, const int8_t w_id, const int8_t ol_number, byte *const buffer,
      const storage::ProjectedRowInitializer &pr_initializer,
      const std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> &pr_map, const catalog::IndexSchema &schema) {
    TERRIER_ASSERT(o_id >= 1 && o_id <= 3000, "Invalid o_id.");
    TERRIER_ASSERT(d_id >= 1 && d_id <= 10, "Invalid d_id.");
    TERRIER_ASSERT(w_id >= 1, "Invalid w_id.");
    TERRIER_ASSERT(buffer != nullptr, "buffer is nullptr.");

    auto *const pr = pr_initializer.InitializeRow(buffer);

    uint32_t col_offset = 0;

    // Primary Key: (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_w_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, w_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_d_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, d_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_o_id", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, o_id);
    TERRIER_ASSERT(schema.GetColumn(col_offset).Name() == "ol_number", "Wrong attribute.");
    Util::SetKeyAttribute(schema, col_offset++, pr_map, pr, ol_number);

    TERRIER_ASSERT(col_offset == schema.GetColumns().size(), "Didn't get every attribute for Order Line key.");

    return pr;
  }
};

}  // namespace terrier::tpcc
