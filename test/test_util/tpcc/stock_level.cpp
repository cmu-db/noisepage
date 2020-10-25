#include "test_util/tpcc/stock_level.h"

#include <unordered_map>
#include <vector>

namespace noisepage::tpcc {

// 2.8.2
bool StockLevel::Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
                         const TransactionArgs &args) const {
  NOISEPAGE_ASSERT(args.type_ == TransactionType::StockLevel, "Wrong transaction type.");
  // ARGS: W_ID, D_ID, S_QUANTITY_THRESHOLD

  auto *const txn = txn_manager->BeginTransaction();
  std::vector<storage::TupleSlot> index_scan_results;

  // Look up D_W_ID and D_ID, retrieve D_NEXT_O_ID
  const auto district_key_pr_initializer = db->district_primary_index_->GetProjectedRowInitializer();
  auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer_);

  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset_)) = args.w_id_;

  index_scan_results.clear();
  db->district_primary_index_->ScanKey(*txn, *district_key, &index_scan_results);
  NOISEPAGE_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

  auto *district_select_tuple = district_select_pr_initializer_.InitializeRow(worker->district_tuple_buffer_);
  bool select_result UNUSED_ATTRIBUTE =
      db->district_table_->Select(common::ManagedPointer(txn), index_scan_results[0], district_select_tuple);
  NOISEPAGE_ASSERT(select_result, "District should be present.");

  const auto d_next_o_id = *reinterpret_cast<int32_t *>(district_select_tuple->AccessWithNullCheck(0));
  NOISEPAGE_ASSERT(d_next_o_id >= 3001, "Invalid d_next_o_id read from the District table.");

  // Select all matching OL_W_ID and OL_D_ID and OL_O_ID in range [D_NEXT_O_ID - 20, D_NEXT_OID)
  const auto order_line_key_pr_initializer = db->order_line_primary_index_->GetProjectedRowInitializer();
  auto *const order_line_key_lo = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer_);
  auto *const order_line_key_hi = order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer_);

  *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_o_id_key_pr_offset_)) = d_next_o_id - 20;
  *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_number_key_pr_offset_)) = 1;

  *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_o_id_key_pr_offset_)) = d_next_o_id - 1;
  *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_number_key_pr_offset_)) = 15;  // max OL_NUMBER

  index_scan_results.clear();
  db->order_line_primary_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 4, order_line_key_lo,
                                               order_line_key_hi, 0, &index_scan_results);
  NOISEPAGE_ASSERT(index_scan_results.size() >= 100 && index_scan_results.size() <= 300,
                   "ol_number can be between 5 and 15, and we're looking up 20 previous orders.");

  // Select matching S_I_ID and S_W_ID with S_QUANTITY lower than threshold.
  // Aggregate quantity counts, report number of items with count < threshold.
  std::unordered_map<int32_t, int32_t> item_counts;

  for (const auto &order_line_tuple_slot : index_scan_results) {
    storage::ProjectedRow *order_line_select_tuple =
        order_line_select_pr_initializer_.InitializeRow(worker->order_line_tuple_buffer_);
    select_result =
        db->order_line_table_->Select(common::ManagedPointer(txn), order_line_tuple_slot, order_line_select_tuple);
    NOISEPAGE_ASSERT(select_result, "Order line index contained this.");
    const auto ol_i_id = *reinterpret_cast<int32_t *>(order_line_select_tuple->AccessForceNotNull(0));
    NOISEPAGE_ASSERT(ol_i_id >= 1 && ol_i_id <= 100000, "Invalid ol_i_id read from the Order Line table.");

    if (item_counts.count(ol_i_id) > 0) continue;  // don't look up items we've already checked

    const auto stock_key_pr_initializer = db->stock_primary_index_->GetProjectedRowInitializer();
    auto *const stock_key = stock_key_pr_initializer.InitializeRow(worker->stock_key_buffer_);
    *reinterpret_cast<int8_t *>(stock_key->AccessForceNotNull(s_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int32_t *>(stock_key->AccessForceNotNull(s_i_id_key_pr_offset_)) = ol_i_id;

    std::vector<storage::TupleSlot> stock_index_scan_results;
    stock_index_scan_results.clear();
    db->stock_primary_index_->ScanKey(*txn, *stock_key, &stock_index_scan_results);
    NOISEPAGE_ASSERT(stock_index_scan_results.size() == 1, "Couldn't find a matching stock item.");

    auto *const stock_select_tuple = stock_select_pr_initializer_.InitializeRow(worker->stock_tuple_buffer_);
    select_result =
        db->stock_table_->Select(common::ManagedPointer(txn), stock_index_scan_results[0], stock_select_tuple);
    NOISEPAGE_ASSERT(select_result, "Stock index contained this.");
    const auto s_quantity = *reinterpret_cast<int16_t *>(stock_select_tuple->AccessForceNotNull(0));
    NOISEPAGE_ASSERT(s_quantity >= 10 && s_quantity <= 100, "Invalid s_quantity read from the Stock table.");

    item_counts[ol_i_id] = s_quantity;
  }

  uint16_t low_stock = 0;
  for (const auto &it : item_counts) {
    low_stock = static_cast<uint16_t>(low_stock + static_cast<uint16_t>(it.second < args.s_quantity_threshold_));
  }

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return true;
}

}  // namespace noisepage::tpcc
