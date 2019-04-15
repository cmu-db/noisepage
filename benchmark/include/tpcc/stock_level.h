#pragma once

#include <unordered_map>

#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/database.h"
#include "tpcc/util.h"
#include "tpcc/worker.h"
#include "tpcc/workload.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

class StockLevel {
 private:
  const storage::ProjectedRowInitializer district_select_pr_initializer;
  const uint8_t d_id_key_pr_offset;
  const uint8_t d_w_id_key_pr_offset;

  const storage::ProjectedRowInitializer order_line_select_pr_initializer;

  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;

  const storage::ProjectedRowInitializer stock_select_pr_initializer;

  const uint8_t s_w_id_key_pr_offset;
  const uint8_t s_i_id_key_pr_offset;

 public:
  explicit StockLevel(const Database *const db)
      : district_select_pr_initializer(
            db->district_table_->InitializerForProjectedRow({db->district_schema_.GetColumn(10).GetOid()}).first),
        d_id_key_pr_offset(static_cast<uint8_t>(
            db->district_index_->GetKeyOidToOffsetMap().at(db->district_key_schema_.at(1).GetOid()))),
        d_w_id_key_pr_offset(static_cast<uint8_t>(
            db->district_index_->GetKeyOidToOffsetMap().at(db->district_key_schema_.at(0).GetOid()))),
        order_line_select_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(4).GetOid()}).first),
        ol_o_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(2).GetOid()))),
        ol_d_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(1).GetOid()))),
        ol_w_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(0).GetOid()))),
        ol_number_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(3).GetOid()))),
        stock_select_pr_initializer(
            db->stock_table_->InitializerForProjectedRow({db->stock_schema_.GetColumn(2).GetOid()}).first),
        s_w_id_key_pr_offset(
            static_cast<uint8_t>(db->stock_index_->GetKeyOidToOffsetMap().at(db->stock_key_schema_.at(0).GetOid()))),
        s_i_id_key_pr_offset(
            static_cast<uint8_t>(db->stock_index_->GetKeyOidToOffsetMap().at(db->stock_key_schema_.at(1).GetOid()))) {}

  // 2.4.2
  template <class Random>
  bool Execute(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
               Worker *const worker, const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::StockLevel, "Wrong transaction type.");
    // ARGS: W_ID, D_ID, S_QUANTITY_THRESHOLD

    auto *const txn = txn_manager->BeginTransaction();
    std::vector<storage::TupleSlot> index_scan_results;

    // Look up D_W_ID and D_ID, retrieve D_NEXT_O_ID
    const auto district_key_pr_initializer = db->district_index_->GetProjectedRowInitializer();
    auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer);

    *reinterpret_cast<int32_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset)) = args.w_id;

    index_scan_results.clear();
    db->district_index_->ScanKey(*district_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

    auto *district_select_tuple = district_select_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    bool select_result UNUSED_ATTRIBUTE =
        db->district_table_->Select(txn, index_scan_results[0], district_select_tuple);
    TERRIER_ASSERT(select_result, "District should be present.");

    const auto d_next_o_id = *reinterpret_cast<int32_t *>(district_select_tuple->AccessWithNullCheck(0));

    // Select all matching OL_W_ID and OL_D_ID and OL_O_ID in range [D_NEXT_O_ID - 20, D_NEXT_OID)
    const auto order_line_key_pr_initializer = db->order_line_index_->GetProjectedRowInitializer();
    auto *const order_line_key_lo = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer);
    auto *const order_line_key_hi = order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);

    *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
    *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_o_id_key_pr_offset)) = d_next_o_id - 20;
    *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_number_key_pr_offset)) = 1;

    *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
    *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_o_id_key_pr_offset)) = d_next_o_id - 1;
    *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_number_key_pr_offset)) = 15;  // max OL_NUMBER

    index_scan_results.clear();
    db->order_line_index_->Scan(*order_line_key_lo, *order_line_key_hi, &index_scan_results);
    // TODO(WAN): what should this assert be?

    // Select matching S_I_ID and S_W_ID with S_QUANTITY lower than threshold.
    // Aggregate quantity counts, report number of items with count < threshold.
    std::unordered_map<int32_t, int32_t> item_counts;

    for (const auto &order_line_tuple_slot : index_scan_results) {
      storage::ProjectedRow *order_line_select_tuple =
          order_line_select_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);
      select_result = db->order_line_table_->Select(txn, order_line_tuple_slot, order_line_select_tuple);
      TERRIER_ASSERT(select_result, "Order line index contained this.");
      const auto ol_i_id = *reinterpret_cast<int32_t *>(order_line_select_tuple->AccessForceNotNull(0));

      const auto stock_key_pr_initializer = db->stock_index_->GetProjectedRowInitializer();
      auto *const stock_key = stock_key_pr_initializer.InitializeRow(worker->stock_key_buffer);
      *reinterpret_cast<int32_t *>(stock_key->AccessForceNotNull(s_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(stock_key->AccessForceNotNull(s_i_id_key_pr_offset)) = ol_i_id;

      std::vector<storage::TupleSlot> stock_index_scan_results;
      stock_index_scan_results.clear();
      db->stock_index_->ScanKey(*stock_key, &stock_index_scan_results);
      TERRIER_ASSERT(stock_index_scan_results.size() == 1, "Couldn't find a matching stock item.");

      auto *const stock_select_tuple = stock_select_pr_initializer.InitializeRow(worker->stock_tuple_buffer);
      select_result = db->stock_table_->Select(txn, stock_index_scan_results[0], stock_select_tuple);
      TERRIER_ASSERT(select_result, "Stock index contained this.");
      const auto s_quantity = *reinterpret_cast<int16_t *>(stock_select_tuple->AccessForceNotNull(0));

      item_counts[ol_i_id] = static_cast<int16_t>(item_counts[ol_i_id] + s_quantity);  // default of int32_t is 0
    }

    uint8_t low_stock = 0;
    for (const auto &it : item_counts) {
      low_stock = static_cast<uint8_t>(low_stock + static_cast<uint8_t>(it.second < args.s_quantity_threshold));
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    return true;
  }
};

}  // namespace terrier::tpcc
