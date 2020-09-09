#include "test_util/tpcc/new_order.h"

#include <string>
#include <vector>

namespace terrier::tpcc {

// 2.4.2
bool NewOrder::Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
                       const TransactionArgs &args) const {
  TERRIER_ASSERT(args.type_ == TransactionType::NewOrder, "Wrong transaction type.");

  double UNUSED_ATTRIBUTE total_amount = 0;

  auto *const txn = txn_manager->BeginTransaction();

  // Look up W_ID in index
  const auto warehouse_key_pr_initializer = db->warehouse_primary_index_->GetProjectedRowInitializer();
  auto *const warehouse_key = warehouse_key_pr_initializer.InitializeRow(worker->warehouse_key_buffer_);

  *reinterpret_cast<int8_t *>(warehouse_key->AccessForceNotNull(0)) = args.w_id_;

  std::vector<storage::TupleSlot> index_scan_results;
  db->warehouse_primary_index_->ScanKey(*txn, *warehouse_key, &index_scan_results);
  TERRIER_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

  // Select W_TAX in table
  auto *const warehouse_select_tuple = warehouse_select_pr_initializer_.InitializeRow(worker->warehouse_tuple_buffer_);
  bool UNUSED_ATTRIBUTE select_result =
      db->warehouse_table_->Select(common::ManagedPointer(txn), index_scan_results[0], warehouse_select_tuple);
  TERRIER_ASSERT(select_result, "Warehouse table doesn't change. All lookups should succeed.");
  const auto w_tax = *reinterpret_cast<double *>(warehouse_select_tuple->AccessWithNullCheck(0));
  TERRIER_ASSERT(w_tax >= 0 && w_tax <= 0.2, "Invalid w_tax read from the Warehouse table.");

  // Look up D_ID, W_ID in index
  const auto district_key_pr_initializer = db->district_primary_index_->GetProjectedRowInitializer();
  auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer_);

  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset_)) = args.w_id_;

  index_scan_results.clear();
  db->district_primary_index_->ScanKey(*txn, *district_key, &index_scan_results);
  TERRIER_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

  // Select D_TAX, D_NEXT_O_ID in table
  auto *const district_select_tuple = district_select_pr_initializer_.InitializeRow(worker->district_tuple_buffer_);
  select_result =
      db->district_table_->Select(common::ManagedPointer(txn), index_scan_results[0], district_select_tuple);
  TERRIER_ASSERT(select_result, "District table doesn't change. All lookups should succeed.");

  const auto d_tax = *reinterpret_cast<double *>(district_select_tuple->AccessWithNullCheck(d_tax_select_pr_offset_));
  const auto d_next_o_id =
      *reinterpret_cast<int32_t *>(district_select_tuple->AccessWithNullCheck(d_next_o_id_select_pr_offset_));
  TERRIER_ASSERT(d_tax >= 0 && d_tax <= 0.2, "Invalid d_tax read from the District table.");
  TERRIER_ASSERT(d_next_o_id >= 3001, "Invalid d_next_o_id read from the District table.");

  // Increment D_NEXT_O_ID in table
  auto *const district_update_redo =
      txn->StageWrite(db->db_oid_, db->district_table_oid_, district_update_pr_initializer_);
  *reinterpret_cast<int32_t *>(district_update_redo->Delta()->AccessForceNotNull(0)) = d_next_o_id + 1;
  district_update_redo->SetTupleSlot(index_scan_results[0]);
  bool UNUSED_ATTRIBUTE result = db->district_table_->Update(common::ManagedPointer(txn), district_update_redo);
  TERRIER_ASSERT(result, "District update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

  // Look up C_ID, D_ID, W_ID in index
  const auto customer_key_pr_initializer = db->customer_primary_index_->GetProjectedRowInitializer();
  auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer_);

  *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset_)) = args.c_id_;
  *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset_)) = args.w_id_;

  index_scan_results.clear();
  db->customer_primary_index_->ScanKey(*txn, *customer_key, &index_scan_results);
  TERRIER_ASSERT(index_scan_results.size() == 1, "Customer index lookup failed.");

  // Select C_DISCOUNT, C_LAST, and C_CREDIT in table
  auto *const customer_select_tuple = customer_select_pr_initializer_.InitializeRow(worker->customer_tuple_buffer_);
  select_result =
      db->customer_table_->Select(common::ManagedPointer(txn), index_scan_results[0], customer_select_tuple);
  TERRIER_ASSERT(select_result, "Customer table doesn't change. All lookups should succeed.");
  const auto c_discount =
      *reinterpret_cast<double *>(customer_select_tuple->AccessWithNullCheck(c_discount_select_pr_offset_));
  TERRIER_ASSERT(c_discount >= 0 && c_discount <= 0.5, "Invalid c_discount read from the Customer table.");

  // Insert new row in New Order
  auto *const new_order_insert_redo =
      txn->StageWrite(db->db_oid_, db->new_order_table_oid_, new_order_insert_pr_initializer_);
  auto *const new_order_insert_tuple = new_order_insert_redo->Delta();

  *reinterpret_cast<int32_t *>(new_order_insert_tuple->AccessForceNotNull(no_o_id_insert_pr_offset_)) = d_next_o_id;
  *reinterpret_cast<int8_t *>(new_order_insert_tuple->AccessForceNotNull(no_d_id_insert_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(new_order_insert_tuple->AccessForceNotNull(no_w_id_insert_pr_offset_)) = args.w_id_;

  const auto new_order_slot = db->new_order_table_->Insert(common::ManagedPointer(txn), new_order_insert_redo);

  // insert in New Order index
  const auto new_order_key_pr_initializer = db->new_order_primary_index_->GetProjectedRowInitializer();
  auto *const new_order_key = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer_);

  *reinterpret_cast<int32_t *>(new_order_key->AccessForceNotNull(no_o_id_key_pr_offset_)) = d_next_o_id;
  *reinterpret_cast<int8_t *>(new_order_key->AccessForceNotNull(no_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(new_order_key->AccessForceNotNull(no_w_id_key_pr_offset_)) = args.w_id_;

  bool UNUSED_ATTRIBUTE index_insert_result =
      db->new_order_primary_index_->InsertUnique(common::ManagedPointer(txn), *new_order_key, new_order_slot);
  TERRIER_ASSERT(index_insert_result, "New Order index insertion failed.");

  // Insert new row in Order
  auto *const order_insert_redo = txn->StageWrite(db->db_oid_, db->order_table_oid_, order_insert_pr_initializer_);
  auto *const order_insert_tuple = order_insert_redo->Delta();

  *reinterpret_cast<int32_t *>(order_insert_tuple->AccessForceNotNull(o_id_insert_pr_offset_)) = d_next_o_id;
  *reinterpret_cast<int8_t *>(order_insert_tuple->AccessForceNotNull(o_d_id_insert_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(order_insert_tuple->AccessForceNotNull(o_w_id_insert_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int32_t *>(order_insert_tuple->AccessForceNotNull(o_c_id_insert_pr_offset_)) = args.c_id_;
  *reinterpret_cast<uint64_t *>(order_insert_tuple->AccessForceNotNull(o_entry_d_insert_pr_offset_)) = args.o_entry_d_;
  order_insert_tuple->SetNull(o_carrier_id_insert_pr_offset_);
  *reinterpret_cast<int8_t *>(order_insert_tuple->AccessForceNotNull(o_ol_cnt_insert_pr_offset_)) = args.ol_cnt_;
  *reinterpret_cast<int8_t *>(order_insert_tuple->AccessForceNotNull(o_all_local_insert_pr_offset_)) =
      static_cast<int8_t>(args.o_all_local_);

  const auto order_slot = db->order_table_->Insert(common::ManagedPointer(txn), order_insert_redo);

  // insert in Order index
  const auto order_key_pr_initializer = db->order_primary_index_->GetProjectedRowInitializer();
  auto *const order_key = order_key_pr_initializer.InitializeRow(worker->order_key_buffer_);

  *reinterpret_cast<int32_t *>(order_key->AccessForceNotNull(o_id_key_pr_offset_)) = d_next_o_id;
  *reinterpret_cast<int8_t *>(order_key->AccessForceNotNull(o_d_id_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(order_key->AccessForceNotNull(o_w_id_key_pr_offset_)) = args.w_id_;

  index_insert_result = db->order_primary_index_->InsertUnique(common::ManagedPointer(txn), *order_key, order_slot);
  TERRIER_ASSERT(index_insert_result, "Order index insertion failed.");

  // insert in Order secondary index
  const auto order_secondary_key_pr_initializer = db->order_secondary_index_->GetProjectedRowInitializer();
  auto *const order_secondary_key =
      order_secondary_key_pr_initializer.InitializeRow(worker->order_secondary_key_buffer_);

  *reinterpret_cast<int32_t *>(order_secondary_key->AccessForceNotNull(o_id_secondary_key_pr_offset_)) = d_next_o_id;
  *reinterpret_cast<int8_t *>(order_secondary_key->AccessForceNotNull(o_d_id_secondary_key_pr_offset_)) = args.d_id_;
  *reinterpret_cast<int8_t *>(order_secondary_key->AccessForceNotNull(o_w_id_secondary_key_pr_offset_)) = args.w_id_;
  *reinterpret_cast<int32_t *>(order_secondary_key->AccessForceNotNull(o_c_id_secondary_key_pr_offset_)) = args.c_id_;

  index_insert_result =
      db->order_secondary_index_->InsertUnique(common::ManagedPointer(txn), *order_secondary_key, order_slot);
  TERRIER_ASSERT(index_insert_result, "Order secondary index insertion failed.");

  // for each item in order
  int8_t ol_number = 1;
  for (const auto &item : args.items_) {
    // Look up I_ID in index
    const auto item_key_pr_initializer = db->item_primary_index_->GetProjectedRowInitializer();
    auto *const item_key = item_key_pr_initializer.InitializeRow(worker->item_key_buffer_);
    *reinterpret_cast<int32_t *>(item_key->AccessForceNotNull(0)) = item.ol_i_id_;
    index_scan_results.clear();
    db->item_primary_index_->ScanKey(*txn, *item_key, &index_scan_results);

    if (index_scan_results.empty()) {
      TERRIER_ASSERT(item.ol_i_id_ == 8491138, "It's the unused value.");
      txn_manager->Abort(txn);
      return false;
    }

    TERRIER_ASSERT(index_scan_results.size() == 1, "Item index lookup failed.");

    // Select I_PRICE, I_NAME, and I_DATE in table
    auto *const item_select_tuple = item_select_pr_initializer_.InitializeRow(worker->item_tuple_buffer_);
    select_result = db->item_table_->Select(common::ManagedPointer(txn), index_scan_results[0], item_select_tuple);
    TERRIER_ASSERT(select_result, "Item table doesn't change. All lookups should succeed.");
    const auto i_price = *reinterpret_cast<double *>(item_select_tuple->AccessWithNullCheck(i_price_select_pr_offset_));
    const auto i_data =
        *reinterpret_cast<storage::VarlenEntry *>(item_select_tuple->AccessWithNullCheck(i_data_select_pr_offset_));
    TERRIER_ASSERT(i_price >= 1.0 && i_price <= 100.0, "Invalid i_price read from the Item table.");

    // Look up S_I_ID, S_W_ID in index
    const auto stock_key_pr_initializer = db->stock_primary_index_->GetProjectedRowInitializer();
    auto *const stock_key = stock_key_pr_initializer.InitializeRow(worker->stock_key_buffer_);

    *reinterpret_cast<int32_t *>(stock_key->AccessForceNotNull(s_i_id_key_pr_offset_)) = item.ol_i_id_;
    *reinterpret_cast<int8_t *>(stock_key->AccessForceNotNull(s_w_id_key_pr_offset_)) = item.ol_supply_w_id_;

    index_scan_results.clear();
    db->stock_primary_index_->ScanKey(*txn, *stock_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Stock index lookup failed.");

    // Select S_QUANTITY, S_DIST_xx (xx = args.d_id), S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA in table
    auto *const stock_select_tuple =
        stock_select_initializers_[args.d_id_ - 1].first.InitializeRow(worker->stock_tuple_buffer_);
    select_result = db->stock_table_->Select(common::ManagedPointer(txn), index_scan_results[0], stock_select_tuple);
    TERRIER_ASSERT(select_result, "Stock table doesn't change (no new entries). All lookups should succeed.");
    const auto s_quantity = *reinterpret_cast<int16_t *>(
        stock_select_tuple->AccessWithNullCheck(stock_select_pr_offsets_[args.d_id_ - 1].s_quantity_select_pr_offset_));
    const auto s_dist_xx = *reinterpret_cast<storage::VarlenEntry *>(
        stock_select_tuple->AccessWithNullCheck(stock_select_pr_offsets_[args.d_id_ - 1].s_dist_xx_select_pr_offset_));
    const auto s_ytd = *reinterpret_cast<int32_t *>(
        stock_select_tuple->AccessWithNullCheck(stock_select_pr_offsets_[args.d_id_ - 1].s_ytd_select_pr_offset_));
    const auto s_order_cnt = *reinterpret_cast<int16_t *>(stock_select_tuple->AccessWithNullCheck(
        stock_select_pr_offsets_[args.d_id_ - 1].s_order_cnt_select_pr_offset_));
    const auto s_remote_cnt = *reinterpret_cast<int16_t *>(stock_select_tuple->AccessWithNullCheck(
        stock_select_pr_offsets_[args.d_id_ - 1].s_remote_cnt_select_pr_offset_));
    const auto s_data = *reinterpret_cast<storage::VarlenEntry *>(
        stock_select_tuple->AccessWithNullCheck(stock_select_pr_offsets_[args.d_id_ - 1].s_data_select_pr_offset_));

    // Update S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT
    auto *const stock_update_redo = txn->StageWrite(db->db_oid_, db->stock_table_oid_, stock_update_pr_initializer_);
    auto *const stock_update_tuple = stock_update_redo->Delta();

    *reinterpret_cast<int16_t *>(stock_update_tuple->AccessForceNotNull(s_quantity_update_pr_offset_)) =
        static_cast<int16_t>((s_quantity >= item.ol_quantity_ + 10) ? s_quantity - item.ol_quantity_
                                                                    : s_quantity - item.ol_quantity_ + 91);
    *reinterpret_cast<int32_t *>(stock_update_tuple->AccessForceNotNull(s_ytd_update_pr_offset_)) =
        s_ytd + item.ol_quantity_;
    *reinterpret_cast<int16_t *>(stock_update_tuple->AccessForceNotNull(s_order_cnt_update_pr_offset_)) =
        static_cast<int16_t>(s_order_cnt + 1);
    *reinterpret_cast<int16_t *>(stock_update_tuple->AccessForceNotNull(s_remote_cnt_update_pr_offset_)) =
        static_cast<int16_t>(item.remote_ ? s_remote_cnt + 1 : s_remote_cnt);
    stock_update_redo->SetTupleSlot(index_scan_results[0]);
    result = db->stock_table_->Update(common::ManagedPointer(txn), stock_update_redo);
    if (!result) {
      // This can fail due to remote orders
      txn_manager->Abort(txn);
      return false;
    }

    const double ol_amount = item.ol_quantity_ * i_price;

    const auto i_data_str = i_data.StringView();
    const auto s_data_str = s_data.StringView();

    const std::string UNUSED_ATTRIBUTE brand_generic =
        (i_data_str.find("ORIGINAL", 0) != std::string::npos && s_data_str.find("ORIGINAL", 0) != std::string::npos)
            ? "B"
            : "G";

    // Insert new row in Order Line
    auto *const order_line_insert_redo =
        txn->StageWrite(db->db_oid_, db->order_line_table_oid_, order_line_insert_pr_initializer_);
    auto *const order_line_insert_tuple = order_line_insert_redo->Delta();

    *reinterpret_cast<int32_t *>(order_line_insert_tuple->AccessForceNotNull(ol_o_id_insert_pr_offset_)) = d_next_o_id;
    *reinterpret_cast<int8_t *>(order_line_insert_tuple->AccessForceNotNull(ol_d_id_insert_pr_offset_)) = args.d_id_;
    *reinterpret_cast<int8_t *>(order_line_insert_tuple->AccessForceNotNull(ol_w_id_insert_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(order_line_insert_tuple->AccessForceNotNull(ol_number_insert_pr_offset_)) = ol_number;
    *reinterpret_cast<int32_t *>(order_line_insert_tuple->AccessForceNotNull(ol_i_id_insert_pr_offset_)) =
        item.ol_i_id_;
    *reinterpret_cast<int8_t *>(order_line_insert_tuple->AccessForceNotNull(ol_supply_w_id_insert_pr_offset_)) =
        item.ol_supply_w_id_;
    order_line_insert_tuple->SetNull(ol_delivery_d_insert_pr_offset_);
    *reinterpret_cast<int8_t *>(order_line_insert_tuple->AccessForceNotNull(ol_quantity_insert_pr_offset_)) =
        item.ol_quantity_;
    *reinterpret_cast<double *>(order_line_insert_tuple->AccessForceNotNull(ol_amount_insert_pr_offset_)) =
        item.ol_quantity_ * i_price;

    if (s_dist_xx.Size() <= storage::VarlenEntry::InlineThreshold()) {
      *reinterpret_cast<storage::VarlenEntry *>(
          order_line_insert_tuple->AccessForceNotNull(ol_dist_info_insert_pr_offset_)) = s_dist_xx;

    } else {
      auto *const varlen = common::AllocationUtil::AllocateAligned(s_dist_xx.Size());
      std::memcpy(varlen, s_dist_xx.Content(), s_dist_xx.Size());
      const auto varlen_entry = storage::VarlenEntry::Create(varlen, s_dist_xx.Size(), true);
      *reinterpret_cast<storage::VarlenEntry *>(
          order_line_insert_tuple->AccessForceNotNull(ol_dist_info_insert_pr_offset_)) = varlen_entry;
    }

    const auto order_line_slot = db->order_line_table_->Insert(common::ManagedPointer(txn), order_line_insert_redo);

    // insert in Order Line index
    const auto order_line_key_pr_initializer = db->order_line_primary_index_->GetProjectedRowInitializer();
    auto *const order_line_key = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer_);

    *reinterpret_cast<int8_t *>(order_line_key->AccessForceNotNull(ol_w_id_key_pr_offset_)) = args.w_id_;
    *reinterpret_cast<int8_t *>(order_line_key->AccessForceNotNull(ol_d_id_key_pr_offset_)) = args.d_id_;
    *reinterpret_cast<int32_t *>(order_line_key->AccessForceNotNull(ol_o_id_key_pr_offset_)) = d_next_o_id;
    *reinterpret_cast<int8_t *>(order_line_key->AccessForceNotNull(ol_number_key_pr_offset_)) = ol_number;

    index_insert_result =
        db->order_line_primary_index_->InsertUnique(common::ManagedPointer(txn), *order_line_key, order_line_slot);
    TERRIER_ASSERT(index_insert_result, "Order Line index insertion failed.");

    ol_number++;
    total_amount += ol_amount;
  }

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  total_amount = total_amount * (1 - c_discount) * (1 + w_tax + d_tax);

  return true;
}

}  // namespace terrier::tpcc
