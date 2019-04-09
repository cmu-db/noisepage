#pragma once

#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/database.h"
#include "tpcc/loader.h"
#include "tpcc/util.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

struct NewOrderArgs {
  struct NO_Item {
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    bool home;
  };

  int32_t w_id;
  int32_t d_id;
  int32_t c_id;
  int32_t ol_cnt;
  uint8_t rbk;
  std::vector<NO_Item> items;
  uint64_t o_entry_d;
  bool o_all_local;
};

// 2.4.1
template <class Random>
NewOrderArgs BuildNewOrderArgs(Random *const generator, const int32_t w_id) {
  TERRIER_ASSERT(w_id >= 1 && w_id <= num_warehouses_, "Invalid w_id.");
  NewOrderArgs args;
  args.w_id = w_id;
  args.d_id = Util::RandomWithin<int32_t>(1, 10, 0, generator);
  args.c_id = Util::NURand(1023, 1, 3000, generator);
  args.ol_cnt = Util::RandomWithin<int32_t>(5, 15, 0, generator);
  args.rbk = Util::RandomWithin<uint8_t>(1, 100, 0, generator);
  args.o_all_local = true;

  args.items.reserve(args.ol_cnt);

  for (int32_t i = 0; i < args.ol_cnt; i++) {
    int32_t ol_i_id = (i == args.ol_cnt - 1 && args.rbk == 1) ? 8491138 : Util::NURand(8191, 1, 100000, generator);
    int32_t ol_supply_w_id;
    bool home;
    if (num_warehouses_ == 1 || Util::RandomWithin<uint8_t>(1, 100, 0, generator) > 1) {
      ol_supply_w_id = w_id;
      home = true;
    } else {
      int32_t remote_w_id;
      do {
        remote_w_id = Util::RandomWithin<uint8_t>(1, num_warehouses_, 0, generator);
      } while (remote_w_id == w_id);
      ol_supply_w_id = remote_w_id;
      home = false;
      args.o_all_local = false;
    }
    int32_t ol_quantity = Util::RandomWithin<uint8_t>(1, 10, 0, generator);
    args.items.push_back({ol_i_id, ol_supply_w_id, ol_quantity, home});
  }
  args.o_entry_d = Util::Timestamp();
  return args;
}

struct Transactions {
  Transactions() = delete;

  // 2.4.2
  template <class Random>
  static bool NewOrder(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
                       Worker *const worker, const NewOrderArgs &args) {
    auto *const txn = txn_manager->BeginTransaction();

    // Look up W_ID in index
    const auto warehouse_key_pr_initializer = db->warehouse_index_->GetProjectedRowInitializer();
    const auto warehouse_key_pr_map = db->warehouse_index_->GetKeyOidToOffsetMap();
    const auto *const warehouse_key =
        Loader::BuildWarehouseKey(args.w_id, worker->warehouse_key_buffer, warehouse_key_pr_initializer,
                                  warehouse_key_pr_map, db->warehouse_key_schema_);
    std::vector<storage::TupleSlot> index_scan_results;
    db->warehouse_index_->ScanKey(*warehouse_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

    // Select W_TAX in table
    const auto [warehouse_select_pr_initializer, warehouse_select_pr_map] =
        db->warehouse_table_->InitializerForProjectedRow(
            {db->warehouse_schema_.GetColumn(7).GetOid()});  // TODO(Matt): cache this thing

    auto *const warehouse_select_tuple = warehouse_select_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    db->warehouse_table_->Select(txn, index_scan_results[0], warehouse_select_tuple);
    const auto UNUSED_ATTRIBUTE w_tax = *reinterpret_cast<double *>(warehouse_select_tuple->AccessWithNullCheck(0));

    // Look up D_ID, W_ID in index
    const auto district_key_pr_initializer = db->district_index_->GetProjectedRowInitializer();
    const auto district_key_pr_map = db->district_index_->GetKeyOidToOffsetMap();
    const auto *const district_key =
        Loader::BuildDistrictKey(args.d_id, args.w_id, worker->district_key_buffer, district_key_pr_initializer,
                                 district_key_pr_map, db->district_key_schema_);
    index_scan_results.clear();
    db->district_index_->ScanKey(*district_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

    // Select D_TAX, D_NEXT_O_ID in table
    const auto d_tax_oid = db->district_schema_.GetColumn(8).GetOid();
    const auto d_next_o_id_oid = db->district_schema_.GetColumn(10).GetOid();
    const auto [district_select_pr_initializer, district_select_pr_map] =
        db->district_table_->InitializerForProjectedRow({d_tax_oid, d_next_o_id_oid});  // TODO(Matt): cache this thing
    auto *const district_select_tuple = district_select_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    db->district_table_->Select(txn, index_scan_results[0], district_select_tuple);

    const auto UNUSED_ATTRIBUTE d_tax =
        *reinterpret_cast<double *>(district_select_tuple->AccessWithNullCheck(district_select_pr_map.at(d_tax_oid)));
    const auto UNUSED_ATTRIBUTE d_next_o_id = *reinterpret_cast<int32_t *>(
        district_select_tuple->AccessWithNullCheck(district_select_pr_map.at(d_next_o_id_oid)));

    // Increment D_NEXT_O_ID in table
    const auto [district_update_pr_initializer, district_update_pr_map] =
        db->district_table_->InitializerForProjectedRow({d_next_o_id_oid});  // TODO(Matt): cache this thing
    auto *const district_update_tuple = district_update_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    *reinterpret_cast<int32_t *>(district_update_tuple->AccessForceNotNull(0)) = d_next_o_id + 1;

    bool result = db->district_table_->Update(txn, index_scan_results[0], *district_update_tuple);
    if (!result) {
      txn_manager->Abort(txn);
      return false;
    }

    // Look up C_ID, D_ID, W_ID in index
    const auto customer_key_pr_initializer = db->customer_index_->GetProjectedRowInitializer();
    const auto customer_key_pr_map = db->customer_index_->GetKeyOidToOffsetMap();
    const auto *const customer_key =
        Loader::BuildCustomerKey(args.c_id, args.d_id, args.w_id, worker->customer_key_buffer,
                                 customer_key_pr_initializer, customer_key_pr_map, db->customer_key_schema_);
    index_scan_results.clear();
    db->customer_index_->ScanKey(*customer_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Customer index lookup failed.");

    // Select C_DISCOUNT, C_LAST, and C_CREDIT in table
    const auto c_discount_oid = db->customer_schema_.GetColumn(15).GetOid();
    const auto c_last_oid = db->customer_schema_.GetColumn(5).GetOid();
    const auto c_credit_oid = db->customer_schema_.GetColumn(13).GetOid();
    const auto [customer_select_pr_initializer, customer_select_pr_map] =
        db->customer_table_->InitializerForProjectedRow(
            {c_discount_oid, c_last_oid, c_credit_oid});  // TODO(Matt): cache this thing
    auto *const customer_select_tuple = customer_select_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
    db->customer_table_->Select(txn, index_scan_results[0], customer_select_tuple);
    const auto UNUSED_ATTRIBUTE c_discount = *reinterpret_cast<double *>(
        customer_select_tuple->AccessWithNullCheck(customer_select_pr_map.at(c_discount_oid)));
    const auto UNUSED_ATTRIBUTE c_last = *reinterpret_cast<storage::VarlenEntry *>(
        customer_select_tuple->AccessWithNullCheck(customer_select_pr_map.at(c_last_oid)));
    const auto UNUSED_ATTRIBUTE c_credit = *reinterpret_cast<storage::VarlenEntry *>(
        customer_select_tuple->AccessWithNullCheck(customer_select_pr_map.at(c_credit_oid)));

    // Insert new row in New Order
    const auto [new_order_insert_pr_initializer, new_order_insert_pr_map] =
        db->new_order_table_->InitializerForProjectedRow(
            Util::AllColOidsForSchema(db->new_order_schema_));  // TODO(Matt): cache this thing
    auto *const new_order_insert_tuple = new_order_insert_pr_initializer.InitializeRow(worker->new_order_key_buffer);
    Util::SetTupleAttribute<int32_t>(db->new_order_schema_, 0, new_order_insert_pr_map, new_order_insert_tuple,
                                     d_next_o_id);
    Util::SetTupleAttribute<int32_t>(db->new_order_schema_, 1, new_order_insert_pr_map, new_order_insert_tuple,
                                     args.d_id);
    Util::SetTupleAttribute<int32_t>(db->new_order_schema_, 2, new_order_insert_pr_map, new_order_insert_tuple,
                                     args.w_id);
    const auto new_order_slot = db->new_order_table_->Insert(txn, *new_order_insert_tuple);

    // insert in index
    const auto new_order_key_pr_initializer = db->new_order_index_->GetProjectedRowInitializer();
    const auto new_order_key_pr_map = db->new_order_index_->GetKeyOidToOffsetMap();
    auto *const new_order_key = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer);
    Util::SetKeyAttribute<int32_t>(db->new_order_key_schema_, 0, new_order_key_pr_map, new_order_key, args.w_id);
    Util::SetKeyAttribute<int32_t>(db->new_order_key_schema_, 1, new_order_key_pr_map, new_order_key, args.d_id);
    Util::SetKeyAttribute<int32_t>(db->new_order_key_schema_, 2, new_order_key_pr_map, new_order_key, d_next_o_id);
    bool index_insert_result = db->new_order_index_->ConditionalInsert(
        *new_order_key, new_order_slot, [](const storage::TupleSlot &) { return false; });
    TERRIER_ASSERT(index_insert_result, "New Order index insertion failed.");

    // Insert new row in Order
    const auto [order_insert_pr_initializer, order_insert_pr_map] = db->order_table_->InitializerForProjectedRow(
        Util::AllColOidsForSchema(db->order_schema_));  // TODO(Matt): cache this thing
    auto *const order_insert_tuple = order_insert_pr_initializer.InitializeRow(worker->order_tuple_buffer);
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 0, order_insert_pr_map, order_insert_tuple, d_next_o_id);
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 1, order_insert_pr_map, order_insert_tuple, args.d_id);
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 2, order_insert_pr_map, order_insert_tuple, args.w_id);
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 3, order_insert_pr_map, order_insert_tuple, args.c_id);
    Util::SetTupleAttribute<uint64_t>(db->order_schema_, 4, order_insert_pr_map, order_insert_tuple, args.o_entry_d);
    order_insert_tuple->SetNull(order_insert_pr_map.at(db->order_schema_.GetColumn(5).GetOid()));
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 6, order_insert_pr_map, order_insert_tuple, args.ol_cnt);
    Util::SetTupleAttribute<int32_t>(db->order_schema_, 7, order_insert_pr_map, order_insert_tuple, args.o_all_local);
    const auto order_slot = db->order_table_->Insert(txn, *order_insert_tuple);

    // insert in index
    const auto order_key_pr_initializer = db->order_index_->GetProjectedRowInitializer();
    const auto order_key_pr_map = db->order_index_->GetKeyOidToOffsetMap();
    auto *const order_key = order_key_pr_initializer.InitializeRow(worker->order_key_buffer);

    Util::SetKeyAttribute<int32_t>(db->order_key_schema_, 0, order_key_pr_map, order_key, args.w_id);
    Util::SetKeyAttribute<int32_t>(db->order_key_schema_, 1, order_key_pr_map, order_key, args.d_id);
    Util::SetKeyAttribute<int32_t>(db->order_key_schema_, 2, order_key_pr_map, order_key, d_next_o_id);

    index_insert_result =
        db->order_index_->ConditionalInsert(*order_key, order_slot, [](const storage::TupleSlot &) { return false; });
    TERRIER_ASSERT(index_insert_result, "Order index insertion failed.");

    // for each item in order
    for (const auto &item : args.items) {
      // Look up I_ID in index
      const auto item_key_pr_initializer = db->item_index_->GetProjectedRowInitializer();
      const auto item_key_pr_map = db->item_index_->GetKeyOidToOffsetMap();
      auto *const item_key = item_key_pr_initializer.InitializeRow(worker->item_key_buffer);
      *reinterpret_cast<int32_t *>(item_key->AccessForceNotNull(0)) = item.ol_i_id;
      index_scan_results.clear();
      db->item_index_->ScanKey(*item_key, &index_scan_results);

      if (index_scan_results.empty()) {
        TERRIER_ASSERT(item.ol_i_id == 8491138, "It's the unused value.");
        txn_manager->Abort(txn);
        return false;
      }

      // Select I_PRICE, I_NAME, and I_DATE in table
      const auto i_price_oid = db->item_schema_.GetColumn(3).GetOid();
      const auto i_name_oid = db->item_schema_.GetColumn(2).GetOid();
      const auto i_data_oid = db->item_schema_.GetColumn(4).GetOid();
      const auto [item_select_pr_initializer, item_select_pr_map] = db->item_table_->InitializerForProjectedRow(
          {i_price_oid, i_name_oid, i_data_oid});  // TODO(Matt): cache this thing
      auto *const item_select_tuple = item_select_pr_initializer.InitializeRow(worker->item_tuple_buffer);
      db->item_table_->Select(txn, index_scan_results[0], item_select_tuple);
      const auto UNUSED_ATTRIBUTE i_price =
          *reinterpret_cast<double *>(item_select_tuple->AccessWithNullCheck(item_select_pr_map.at(i_price_oid)));
      const auto UNUSED_ATTRIBUTE i_name = *reinterpret_cast<storage::VarlenEntry *>(
          item_select_tuple->AccessWithNullCheck(item_select_pr_map.at(i_name_oid)));
      const auto UNUSED_ATTRIBUTE i_data = *reinterpret_cast<storage::VarlenEntry *>(
          item_select_tuple->AccessWithNullCheck(item_select_pr_map.at(i_data_oid)));

      // Look up S_I_ID, S_W_ID in index
      const auto stock_key_pr_initializer = db->stock_index_->GetProjectedRowInitializer();
      const auto stock_key_pr_map = db->stock_index_->GetKeyOidToOffsetMap();
      const auto *const stock_key =
          Loader::BuildStockKey(item.ol_i_id, args.w_id, worker->stock_key_buffer, stock_key_pr_initializer,
                                stock_key_pr_map, db->stock_key_schema_);
      index_scan_results.clear();
      db->stock_index_->ScanKey(*stock_key, &index_scan_results);
      TERRIER_ASSERT(index_scan_results.size() == 1, "Stock index lookup failed.");

      // Select S_QUANTITY, S_DIST_xx (xx = args.d_id), S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA in table
      const auto s_quantity_oid = db->stock_schema_.GetColumn(2).GetOid();
      const auto s_dist_xx_oid = db->stock_schema_.GetColumn(2 + args.d_id).GetOid();
      const auto s_ytd_oid = db->stock_schema_.GetColumn(13).GetOid();
      const auto s_order_cnt_oid = db->stock_schema_.GetColumn(14).GetOid();
      const auto s_remote_cnt_oid = db->stock_schema_.GetColumn(15).GetOid();
      const auto s_data_oid = db->stock_schema_.GetColumn(16).GetOid();
      const auto [stock_select_pr_initializer, stock_select_pr_map] =
          db->stock_table_->InitializerForProjectedRow({s_quantity_oid, s_dist_xx_oid, s_ytd_oid, s_order_cnt_oid,
                                                        s_remote_cnt_oid, s_data_oid});  // TODO(Matt): cache this thing
      auto *const stock_select_tuple = stock_select_pr_initializer.InitializeRow(worker->stock_tuple_buffer);
      db->stock_table_->Select(txn, index_scan_results[0], stock_select_tuple);
      const auto UNUSED_ATTRIBUTE s_quantity =
          *reinterpret_cast<int16_t *>(stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_quantity_oid)));
      const auto UNUSED_ATTRIBUTE s_dist_xx = *reinterpret_cast<storage::VarlenEntry *>(
          stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_dist_xx_oid)));
      const auto UNUSED_ATTRIBUTE s_ytd =
          *reinterpret_cast<int32_t *>(stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_ytd_oid)));
      const auto UNUSED_ATTRIBUTE s_order_cnt = *reinterpret_cast<int16_t *>(
          stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_order_cnt_oid)));
      const auto UNUSED_ATTRIBUTE s_remote_cnt = *reinterpret_cast<int16_t *>(
          stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_remote_cnt_oid)));
      const auto UNUSED_ATTRIBUTE s_data = *reinterpret_cast<storage::VarlenEntry *>(
          stock_select_tuple->AccessWithNullCheck(stock_select_pr_map.at(s_data_oid)));

      // Update S_QUANTITY, S_YTD, S_REMOTE_CNT
      const auto [stock_update_pr_initializer, stock_update_pr_map] = db->stock_table_->InitializerForProjectedRow(
          {s_quantity_oid, s_ytd_oid, s_remote_cnt_oid});  // TODO(Matt): cache this thing
      auto *const stock_update_tuple = stock_update_pr_initializer.InitializeRow(worker->stock_tuple_buffer);
      *reinterpret_cast<int16_t *>(stock_update_tuple->AccessForceNotNull(stock_update_pr_map.at(s_quantity_oid))) =
          (s_quantity >= item.ol_quantity + 10) ? s_quantity - item.ol_quantity : s_quantity - item.ol_quantity + 91;
      *reinterpret_cast<int32_t *>(stock_update_tuple->AccessForceNotNull(stock_update_pr_map.at(s_ytd_oid))) =
          s_ytd + item.ol_quantity;
      *reinterpret_cast<int16_t *>(stock_update_tuple->AccessForceNotNull(stock_update_pr_map.at(s_remote_cnt_oid))) =
          !item.home ? s_remote_cnt + 1 : s_remote_cnt;

      result = db->stock_table_->Update(txn, index_scan_results[0], *stock_update_tuple);
      if (!result) {
        txn_manager->Abort(txn);
        return false;
      }
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
