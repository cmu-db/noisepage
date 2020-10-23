#include "test_util/tpcc/payment.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace noisepage::tpcc {

// 2.5.2
bool Payment::Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
                      const TransactionArgs &args) const {
  NOISEPAGE_ASSERT(args.type_ == TransactionType::Payment, "Wrong transaction type.");

  auto *const txn = txn_manager->BeginTransaction();

  // Look up W_ID in index
  const auto warehouse_key_pr_initializer = db->warehouse_primary_index_->GetProjectedRowInitializer();
  auto *const warehouse_key = warehouse_key_pr_initializer.InitializeRow(worker->warehouse_key_buffer_);

  *reinterpret_cast<int8_t *>(warehouse_key->AccessForceNotNull(0)) = args.w_id_;

  std::vector<storage::TupleSlot> index_scan_results;
  db->warehouse_primary_index_->ScanKey(*txn, *warehouse_key, &index_scan_results);
  NOISEPAGE_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

  // Select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD in table
  auto *const warehouse_select_tuple = warehouse_select_pr_initializer_.InitializeRow(worker->warehouse_tuple_buffer_);
  bool UNUSED_ATTRIBUTE select_result =
      db->warehouse_table_->Select(common::ManagedPointer(txn), index_scan_results[0], warehouse_select_tuple);
  NOISEPAGE_ASSERT(select_result, "Warehouse table doesn't change. All lookups should succeed.");
  const auto w_name =
      *reinterpret_cast<storage::VarlenEntry *>(warehouse_select_tuple->AccessWithNullCheck(w_name_select_pr_offset_));
  const auto w_ytd = *reinterpret_cast<double *>(warehouse_select_tuple->AccessWithNullCheck(w_ytd_select_pr_offset_));
  NOISEPAGE_ASSERT(w_ytd >= 300000.0, "Invalid w_ytd read from the Warehouse table.");

  // Increase W_YTD by H_AMOUNT in table
  auto *const warehouse_update_redo =
      txn->StageWrite(db->db_oid_, db->warehouse_table_oid_, warehouse_update_pr_initializer_);
  *reinterpret_cast<double *>(warehouse_update_redo->Delta()->AccessForceNotNull(0)) = w_ytd + args.h_amount_;
  warehouse_update_redo->SetTupleSlot(index_scan_results[0]);
  bool UNUSED_ATTRIBUTE result = db->warehouse_table_->Update(common::ManagedPointer(txn), warehouse_update_redo);
  NOISEPAGE_ASSERT(result,
                   "Warehouse update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

  // Look up D_ID, W_ID in index
  const auto district_key_pr_initializer = db->district_primary_index_->GetProjectedRowInitializer();
  auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer_);

  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset_)) = args.w_id_;

  index_scan_results.clear();
  db->district_primary_index_->ScanKey(*txn, *district_key, &index_scan_results);
  NOISEPAGE_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

  // Select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD in table
  auto *const district_select_tuple = district_select_pr_initializer_.InitializeRow(worker->district_tuple_buffer_);
  select_result =
      db->district_table_->Select(common::ManagedPointer(txn), index_scan_results[0], district_select_tuple);
  NOISEPAGE_ASSERT(select_result, "District table doesn't change. All lookups should succeed.");
  const auto d_name =
      *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_name_select_pr_offset_));
  const auto d_ytd = *reinterpret_cast<double *>(district_select_tuple->AccessWithNullCheck(d_ytd_select_pr_offset_));
  NOISEPAGE_ASSERT(d_ytd >= 30000.0, "Invalid d_ytd read from the District table.");

  // Increase D_YTD by H_AMOUNT in table
  auto *const district_update_redo =
      txn->StageWrite(db->db_oid_, db->district_table_oid_, district_update_pr_initializer_);
  *reinterpret_cast<double *>(district_update_redo->Delta()->AccessForceNotNull(0)) = d_ytd + args.h_amount_;
  district_update_redo->SetTupleSlot(index_scan_results[0]);
  result = db->district_table_->Update(common::ManagedPointer(txn), district_update_redo);
  NOISEPAGE_ASSERT(result, "District update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

  storage::TupleSlot customer_slot;
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
        select_result = db->customer_table_->Select(common::ManagedPointer(txn), tuple_slot, c_first_select_tuple);
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
  select_result = db->customer_table_->Select(common::ManagedPointer(txn), customer_slot, customer_select_tuple);
  NOISEPAGE_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");

  const auto *const c_id_ptr =
      reinterpret_cast<int32_t *>(customer_select_tuple->AccessWithNullCheck(c_id_select_pr_offset_));
  NOISEPAGE_ASSERT(c_id_ptr != nullptr, "This is a non-NULLable field.");
  const auto UNUSED_ATTRIBUTE c_id = !args.use_c_last_ ? args.c_id_ : *c_id_ptr;
  NOISEPAGE_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id read from the Customer table.");

  const auto c_balance =
      *reinterpret_cast<double *>(customer_select_tuple->AccessWithNullCheck(c_balance_select_pr_offset_));
  const auto c_ytd_payment =
      *reinterpret_cast<double *>(customer_select_tuple->AccessWithNullCheck(c_ytd_payment_select_pr_offset_));
  const auto c_payment_cnt =
      *reinterpret_cast<int16_t *>(customer_select_tuple->AccessWithNullCheck(c_payment_cnt_select_pr_offset_));
  const auto c_credit =
      *reinterpret_cast<storage::VarlenEntry *>(customer_select_tuple->AccessWithNullCheck(c_credit_select_pr_offset_));
  const auto c_data =
      *reinterpret_cast<storage::VarlenEntry *>(customer_select_tuple->AccessWithNullCheck(c_data_select_pr_offset_));

  // Update customer
  auto *const customer_update_redo =
      txn->StageWrite(db->db_oid_, db->customer_table_oid_, customer_update_pr_initializer_);
  auto *const customer_update_tuple = customer_update_redo->Delta();
  *reinterpret_cast<double *>(customer_update_tuple->AccessForceNotNull(c_balance_update_pr_offset_)) =
      c_balance - args.h_amount_;
  *reinterpret_cast<double *>(customer_update_tuple->AccessForceNotNull(c_ytd_payment_update_pr_offset_)) =
      c_ytd_payment + args.h_amount_;
  *reinterpret_cast<int16_t *>(customer_update_tuple->AccessForceNotNull(c_payment_cnt_update_pr_offset_)) =
      static_cast<int16_t>(c_payment_cnt + 1);
  customer_update_redo->SetTupleSlot(customer_slot);
  result = db->customer_table_->Update(common::ManagedPointer(txn), customer_update_redo);
  NOISEPAGE_ASSERT(result, "Customer update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

  const auto c_credit_str = c_credit.StringView();
  NOISEPAGE_ASSERT(c_credit_str.compare("BC") == 0 || c_credit_str.compare("GC") == 0,
                   "Invalid c_credit read from the Customer table.");
  if (c_credit_str.compare("BC") == 0) {
    auto *const c_data_update_redo = txn->StageWrite(db->db_oid_, db->customer_table_oid_, c_data_pr_initializer_);

    const auto c_data_str = c_data.StringView();
    auto new_c_data = std::to_string(c_id);
    new_c_data.append(std::to_string(args.c_d_id_));
    new_c_data.append(std::to_string(args.c_w_id_));
    new_c_data.append(std::to_string(args.d_id_));
    new_c_data.append(std::to_string(args.w_id_));
    new_c_data.append(std::to_string(args.h_amount_));
    new_c_data.append(c_data_str);
    const auto new_c_data_length = std::min(new_c_data.length(), static_cast<std::size_t>(500));
    auto *const varlen = common::AllocationUtil::AllocateAligned(new_c_data_length);
    std::memcpy(varlen, new_c_data.data(), new_c_data_length);
    const auto varlen_entry = storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(new_c_data_length), true);

    *reinterpret_cast<storage::VarlenEntry *>(c_data_update_redo->Delta()->AccessForceNotNull(0)) = varlen_entry;

    c_data_update_redo->SetTupleSlot(customer_slot);
    result = db->customer_table_->Update(common::ManagedPointer(txn), c_data_update_redo);
    NOISEPAGE_ASSERT(result,
                     "Customer update failed. This assertion assumes 1:1 mapping between warehouse and workers.");
  }

  auto h_data_str = std::string(reinterpret_cast<const char *const>(w_name.Content()), w_name.Size());
  h_data_str.append("    ");
  h_data_str.append(d_name.StringView());
  const auto h_data_length = h_data_str.length();
  auto *const varlen = common::AllocationUtil::AllocateAligned(h_data_length);
  std::memcpy(varlen, h_data_str.data(), h_data_length);
  const auto h_data = storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(h_data_length), true);

  // Insert in History table
  auto *const history_insert_redo =
      txn->StageWrite(db->db_oid_, db->history_table_oid_, history_insert_pr_initializer_);
  auto *const history_insert_tuple = history_insert_redo->Delta();
  *reinterpret_cast<int32_t *>(history_insert_tuple->AccessForceNotNull(h_c_id_insert_pr_offset_)) = c_id;
  *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_c_d_id_insert_pr_offset_)) = args.c_d_id_;
  *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_c_w_id_insert_pr_offset_)) = args.c_w_id_;
  *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_d_id_insert_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_w_id_insert_pr_offset_)) = args.w_id_;
  *reinterpret_cast<uint64_t *>(history_insert_tuple->AccessForceNotNull(h_date_insert_pr_offset_)) = args.h_date_;
  *reinterpret_cast<double *>(history_insert_tuple->AccessForceNotNull(h_amount_insert_pr_offset_)) = args.h_amount_;
  *reinterpret_cast<storage::VarlenEntry *>(history_insert_tuple->AccessForceNotNull(h_data_insert_pr_offset_)) =
      h_data;

  db->history_table_->Insert(common::ManagedPointer(txn), history_insert_redo);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  return true;
}

}  // namespace noisepage::tpcc
