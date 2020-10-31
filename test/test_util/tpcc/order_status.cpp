#include "test_util/tpcc/order_status.h"

#include <map>
#include <string>
#include <vector>

namespace noisepage::tpcc {

// 2.6.2
bool OrderStatus::Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
                          const TransactionArgs &args) const {
  NOISEPAGE_ASSERT(args.type_ == TransactionType::OrderStatus, "Wrong transaction type.");

  auto *const txn = txn_manager->BeginTransaction();

  storage::TupleSlot customer_slot;
  std::vector<storage::TupleSlot> index_scan_results;
  if (!args.use_c_last_) {
    // Look up C_ID, D_ID, W_ID in index
    const auto customer_key_pr_initializer = db->customer_primary_index_->GetProjectedRowInitializer();
    auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer_);

    *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset_)) = args.c_id_;
    *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset_)) = args.d_id_;
    *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset_)) = args.w_id_;

    index_scan_results.clear();
    db->customer_primary_index_->ScanKey(*txn, *customer_key, &index_scan_results);
    NOISEPAGE_ASSERT(index_scan_results.size() == 1, "Customer index lookup failed.");
    customer_slot = index_scan_results[0];
  } else {
    // Look up C_LAST, D_ID, W_ID in index
    const auto customer_name_key_pr_initializer = db->customer_secondary_index_->GetProjectedRowInitializer();
    auto *const customer_name_key = customer_name_key_pr_initializer.InitializeRow(worker->customer_name_key_buffer_);

    *reinterpret_cast<storage::VarlenEntry *>(customer_name_key->AccessForceNotNull(c_last_name_key_pr_offset_)) =
        args.c_last_;
    *reinterpret_cast<int8_t *>(customer_name_key->AccessForceNotNull(c_d_id_name_key_pr_offset_)) = args.d_id_;
    *reinterpret_cast<int8_t *>(customer_name_key->AccessForceNotNull(c_w_id_name_key_pr_offset_)) = args.w_id_;

    index_scan_results.clear();
    db->customer_secondary_index_->ScanKey(*txn, *customer_name_key, &index_scan_results);
    NOISEPAGE_ASSERT(!index_scan_results.empty(), "Customer Name index lookup failed.");

    if (index_scan_results.size() > 1) {
      std::map<std::string, storage::TupleSlot> sorted_index_scan_results;
      for (const auto &tuple_slot : index_scan_results) {
        auto *const c_first_select_tuple = c_first_pr_initializer_.InitializeRow(worker->customer_tuple_buffer_);
        bool UNUSED_ATTRIBUTE select_result =
            db->customer_table_->Select(common::ManagedPointer(txn), tuple_slot, c_first_select_tuple);
        NOISEPAGE_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");
        const auto c_first = *reinterpret_cast<storage::VarlenEntry *>(c_first_select_tuple->AccessWithNullCheck(0));
        sorted_index_scan_results.emplace(
            std::string(reinterpret_cast<const char *const>(c_first.Content()), c_first.Size()), tuple_slot);
      }

      auto median = sorted_index_scan_results.cbegin();
      std::advance(median, (sorted_index_scan_results.size() + 1) / 2);
      customer_slot = median->second;
    } else {
      customer_slot = index_scan_results[0];
    }
  }

  // Select customer in table
  auto *const customer_select_tuple = customer_select_pr_initializer_.InitializeRow(worker->customer_tuple_buffer_);
  bool UNUSED_ATTRIBUTE select_result =
      db->customer_table_->Select(common::ManagedPointer(txn), customer_slot, customer_select_tuple);
  NOISEPAGE_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");

  const auto *const c_id_ptr =
      reinterpret_cast<int32_t *>(customer_select_tuple->AccessWithNullCheck(c_id_select_pr_offset_));
  NOISEPAGE_ASSERT(c_id_ptr != nullptr, "This is a non-NULLable field.");
  const auto UNUSED_ATTRIBUTE c_id = !args.use_c_last_ ? args.c_id_ : *c_id_ptr;
  NOISEPAGE_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id read from the Customer table.");

  // look up in secondary Order index
  const auto order_secondary_key_pr_initializer = db->order_secondary_index_->GetProjectedRowInitializer();
  auto *const order_secondary_low_key =
      order_secondary_key_pr_initializer.InitializeRow(worker->order_secondary_key_buffer_);
  auto *const order_secondary_high_key =
      order_secondary_key_pr_initializer.InitializeRow(worker->order_tuple_buffer_);  // it's large enough

  *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_id_secondary_key_pr_offset_)) = 1;
  *reinterpret_cast<int8_t *>(order_secondary_low_key->AccessForceNotNull(o_d_id_secondary_key_pr_offset_)) =
      args.d_id_;
  *reinterpret_cast<int8_t *>(order_secondary_low_key->AccessForceNotNull(o_w_id_secondary_key_pr_offset_)) =
      args.w_id_;
  *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_c_id_secondary_key_pr_offset_)) = c_id;

  *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_id_secondary_key_pr_offset_)) = 10000000;
  *reinterpret_cast<int8_t *>(order_secondary_high_key->AccessForceNotNull(o_d_id_secondary_key_pr_offset_)) =
      args.d_id_;
  *reinterpret_cast<int8_t *>(order_secondary_high_key->AccessForceNotNull(o_w_id_secondary_key_pr_offset_)) =
      args.w_id_;
  *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_c_id_secondary_key_pr_offset_)) = c_id;

  index_scan_results.clear();
  db->order_secondary_index_->ScanLimitDescending(*txn, *order_secondary_low_key, *order_secondary_high_key,
                                                  &index_scan_results, 1);
  NOISEPAGE_ASSERT(index_scan_results.size() == 1,
                   "Order index lookup failed. There should always be at least one order for each customer.");

  // Select O_ID, O_ENTRY_D, O_CARRIER_ID from table for largest key (back of vector)
  auto *const order_select_tuple = order_select_pr_initializer_.InitializeRow(worker->order_tuple_buffer_);
  select_result = db->order_table_->Select(common::ManagedPointer(txn), index_scan_results[0], order_select_tuple);
  NOISEPAGE_ASSERT(select_result,
                   "Order select failed. This assertion assumes 1:1 mapping between warehouse and workers.");

  const auto o_id = *reinterpret_cast<int32_t *>(order_select_tuple->AccessWithNullCheck(o_id_select_pr_offset_));

  // look up in Order Line index
  const auto order_line_key_pr_initializer = db->order_line_primary_index_->GetProjectedRowInitializer();
  auto *const order_line_low_key = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer_);
  auto *const order_line_high_key =
      order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer_);  // it's large enough

  *reinterpret_cast<int8_t *>(order_line_low_key->AccessForceNotNull(ol_number_key_pr_offset_)) = 1;
  *reinterpret_cast<int8_t *>(order_line_low_key->AccessForceNotNull(ol_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(order_line_low_key->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int32_t *>(order_line_low_key->AccessForceNotNull(ol_o_id_key_pr_offset_)) = o_id;

  *reinterpret_cast<int8_t *>(order_line_high_key->AccessForceNotNull(ol_number_key_pr_offset_)) = 15;
  *reinterpret_cast<int8_t *>(order_line_high_key->AccessForceNotNull(ol_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(order_line_high_key->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int32_t *>(order_line_high_key->AccessForceNotNull(ol_o_id_key_pr_offset_)) = o_id;

  index_scan_results.clear();
  db->order_line_primary_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 4, order_line_low_key,
                                               order_line_high_key, 0, &index_scan_results);

  NOISEPAGE_ASSERT(!index_scan_results.empty() && index_scan_results.size() <= 15,
                   "There should be at least 1 Order Line item, but no more than 15.");

  // Select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D for every result of the index scan
  auto *const order_line_select_tuple =
      order_line_select_pr_initializer_.InitializeRow(worker->order_line_tuple_buffer_);
  for (const auto &tuple_slot : index_scan_results) {
    select_result = db->order_line_table_->Select(common::ManagedPointer(txn), tuple_slot, order_line_select_tuple);
    NOISEPAGE_ASSERT(select_result,
                     "We already confirmed that this is a committed order above, so none of these should fail.");
  }

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  return true;
}

}  // namespace noisepage::tpcc
