#include "test_util/tpcc/delivery.h"

#include <vector>

namespace noisepage::tpcc {

// 2.7.4
bool Delivery::Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
                       const TransactionArgs &args) const {
  NOISEPAGE_ASSERT(args.type_ == TransactionType::Delivery, "Wrong transaction type.");

  auto *const txn = txn_manager->BeginTransaction();

  for (int8_t d_id = 1; d_id <= 10; d_id++) {
    std::vector<storage::TupleSlot> index_scan_results;

    // Look up NO_W_ID and NO_D_ID, find the lowest NO_O_ID value
    const auto new_order_key_pr_initializer = db->new_order_primary_index_->GetProjectedRowInitializer();
    auto *const new_order_key_lo = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer_);
    auto *const new_order_key_hi = new_order_key_pr_initializer.InitializeRow(worker->new_order_tuple_buffer_);

    *reinterpret_cast<int8_t *>(new_order_key_lo->AccessForceNotNull(no_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(new_order_key_lo->AccessForceNotNull(no_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(new_order_key_lo->AccessForceNotNull(no_o_id_key_pr_offset_)) = 1;

    *reinterpret_cast<int8_t *>(new_order_key_hi->AccessForceNotNull(no_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(new_order_key_hi->AccessForceNotNull(no_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(new_order_key_hi->AccessForceNotNull(no_o_id_key_pr_offset_)) = 10000000;  // max O_ID

    db->new_order_primary_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 3, new_order_key_lo,
                                                new_order_key_hi, 1, &index_scan_results);
    // If no matching row is found, then the delivery is skipped
    if (index_scan_results.empty()) {
      continue;
    }
    // Otherwise, the lowest NO_O_ID is selected
    auto *const new_order_select_tuple = new_order_pr_initializer_.InitializeRow(worker->new_order_tuple_buffer_);
    const auto new_order_slot = index_scan_results[0];
    bool select_result UNUSED_ATTRIBUTE =
        db->new_order_table_->Select(common::ManagedPointer(txn), new_order_slot, new_order_select_tuple);
    NOISEPAGE_ASSERT(select_result,
                     "New Order select failed. This assertion assumes 1:1 mapping between warehouse and workers and "
                     "that indexes are getting cleaned.");
    const auto no_o_id = *reinterpret_cast<int32_t *>(new_order_select_tuple->AccessWithNullCheck(0));

    // Delete the corresponding New Order table row
    txn->StageDelete(db->db_oid_, db->new_order_table_oid_, new_order_slot);
    bool delete_result UNUSED_ATTRIBUTE = db->new_order_table_->Delete(common::ManagedPointer(txn), new_order_slot);
    NOISEPAGE_ASSERT(delete_result,
                     "New Order delete failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    // Delete the New Order index entry. Would need to defer this in a many:1 worker:warehouse scenario
    auto *const new_order_delete_key = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer_);

    *reinterpret_cast<int8_t *>(new_order_delete_key->AccessForceNotNull(no_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(new_order_delete_key->AccessForceNotNull(no_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(new_order_delete_key->AccessForceNotNull(no_o_id_key_pr_offset_)) = no_o_id;
    db->new_order_primary_index_->Delete(common::ManagedPointer(txn), *new_order_delete_key, new_order_slot);

    // Look up O_W_ID, O_D_ID and O_ID
    const auto order_key_pr_initializer = db->order_primary_index_->GetProjectedRowInitializer();
    auto *const order_key = order_key_pr_initializer.InitializeRow(worker->order_key_buffer_);

    *reinterpret_cast<int8_t *>(order_key->AccessForceNotNull(o_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(order_key->AccessForceNotNull(o_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(order_key->AccessForceNotNull(o_id_key_pr_offset_)) = no_o_id;

    index_scan_results.clear();
    db->order_primary_index_->ScanKey(*txn, *order_key, &index_scan_results);
    NOISEPAGE_ASSERT(index_scan_results.size() == 1, "Order index lookup failed.");

    // Retrieve O_C_ID
    auto &order_slot = index_scan_results[0];
    auto *order_select_tuple = order_select_pr_initializer_.InitializeRow(worker->order_tuple_buffer_);
    select_result = db->order_table_->Select(common::ManagedPointer(txn), order_slot, order_select_tuple);
    NOISEPAGE_ASSERT(select_result,
                     "Order select failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    const auto o_c_id = *reinterpret_cast<int32_t *>(order_select_tuple->AccessWithNullCheck(0));
    NOISEPAGE_ASSERT(o_c_id >= 1 && o_c_id <= 3000, "Invalid o_c_id read from the Order table.");

    // update O_CARRIER_ID
    auto *const order_update_redo = txn->StageWrite(db->db_oid_, db->order_table_oid_, order_update_pr_initializer_);
    *reinterpret_cast<int8_t *>(order_update_redo->Delta()->AccessForceNotNull(0)) = args.o_carrier_id_;
    order_update_redo->SetTupleSlot(order_slot);
    bool update_result UNUSED_ATTRIBUTE = db->order_table_->Update(common::ManagedPointer(txn), order_update_redo);
    NOISEPAGE_ASSERT(select_result,
                     "Order update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    // Look up OL_W_ID, OL_D_ID, OL_O_ID
    const auto order_line_key_pr_initializer = db->order_line_primary_index_->GetProjectedRowInitializer();
    auto *const order_line_key_lo = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer_);
    auto *const order_line_key_hi = order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer_);

    *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_o_id_key_pr_offset_)) = no_o_id;
    *reinterpret_cast<int8_t *>(order_line_key_lo->AccessForceNotNull(ol_number_key_pr_offset_)) = 1;

    *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_o_id_key_pr_offset_)) = no_o_id;
    *reinterpret_cast<int8_t *>(order_line_key_hi->AccessForceNotNull(ol_number_key_pr_offset_)) = 15;  // max OL_NUMBER

    index_scan_results.clear();
    db->order_line_primary_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 4, order_line_key_lo,
                                                 order_line_key_hi, 0, &index_scan_results);
    NOISEPAGE_ASSERT(!index_scan_results.empty() && index_scan_results.size() <= 15,
                     "There should be at least 1 Order Line item, but no more than 15.");

    // Retrieve sum of all OL_AMOUNT, update every OL_DELIVERY_D to current system time
    storage::ProjectedRow *order_line_select_tuple;
    double ol_amount_sum = 0.0;
    for (const auto &order_line_slot : index_scan_results) {
      order_line_select_tuple = order_line_select_pr_initializer_.InitializeRow(worker->order_line_tuple_buffer_);
      select_result =
          db->order_line_table_->Select(common::ManagedPointer(txn), order_line_slot, order_line_select_tuple);
      NOISEPAGE_ASSERT(select_result,
                       "Order Line select failed. This assertion assumes 1:1 mapping between warehouse and workers.");
      const auto ol_amount = *reinterpret_cast<double *>(order_line_select_tuple->AccessForceNotNull(0));
      ol_amount_sum += ol_amount;
      NOISEPAGE_ASSERT(ol_amount >= 0.01 && ol_amount <= 9999.99, "Invalid ol_amount read from the Order Line table.");

      auto *const order_line_update_redo =
          txn->StageWrite(db->db_oid_, db->order_line_table_oid_, order_line_update_pr_initializer_);
      *reinterpret_cast<uint64_t *>(order_line_update_redo->Delta()->AccessForceNotNull(0)) = args.ol_delivery_d_;
      order_line_update_redo->SetTupleSlot(order_line_slot);
      update_result = db->order_line_table_->Update(common::ManagedPointer(txn), order_line_update_redo);
      NOISEPAGE_ASSERT(update_result,
                       "Order Line update failed. This assertion assumes 1:1 mapping between warehouse and workers.");
    }

    // Look up C_W_ID, C_D_ID, C_ID
    const auto customer_key_pr_initializer = db->customer_primary_index_->GetProjectedRowInitializer();
    auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer_);

    *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset_)) = d_id;
    *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset_)) = o_c_id;

    // Increase C_BALANCE by OL_AMOUNT, increase C_DELIVERY_CNT
    index_scan_results.clear();
    db->customer_primary_index_->ScanKey(*txn, *customer_key, &index_scan_results);
    NOISEPAGE_ASSERT(index_scan_results.size() == 1, "Customer index scan failed.");

    auto *const customer_update_redo = txn->StageWrite(db->db_oid_, db->customer_table_oid_, customer_pr_initializer_);
    auto *const customer_update_tuple = customer_update_redo->Delta();

    select_result =
        db->customer_table_->Select(common::ManagedPointer(txn), index_scan_results[0], customer_update_tuple);
    NOISEPAGE_ASSERT(select_result,
                     "Customer select failed. This assertion assumes 1:1 mapping between warehouse and workers.");
    *reinterpret_cast<double *>(customer_update_tuple->AccessForceNotNull(c_balance_pr_offset_)) += ol_amount_sum;
    (*reinterpret_cast<int16_t *>(customer_update_tuple->AccessForceNotNull(c_delivery_cnt_pr_offset_)))++;
    customer_update_redo->SetTupleSlot(index_scan_results[0]);
    update_result = db->customer_table_->Update(common::ManagedPointer(txn), customer_update_redo);
    NOISEPAGE_ASSERT(update_result,
                     "Customer update failed. This assertion assumes 1:1 mapping between warehouse and workers.");
  }

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return true;
}

}  // namespace noisepage::tpcc
