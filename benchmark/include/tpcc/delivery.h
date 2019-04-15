#pragma once

#include <algorithm>
#include <map>
#include <string_view>
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

class Delivery {
 private:
  catalog::indexkeycol_oid_t no_o_id_key_oid;
  catalog::indexkeycol_oid_t no_d_id_key_oid;
  catalog::indexkeycol_oid_t no_w_id_key_oid;

  const storage::ProjectedRowInitializer new_order_pr_initializer;
  const uint8_t no_o_id_key_pr_offset;
  const uint8_t no_d_id_key_pr_offset;
  const uint8_t no_w_id_key_pr_offset;

  catalog::indexkeycol_oid_t o_id_key_oid;
  catalog::indexkeycol_oid_t o_d_id_key_oid;
  catalog::indexkeycol_oid_t o_w_id_key_oid;

  const storage::ProjectedRowInitializer order_select_pr_initializer;
  const storage::ProjectedRowInitializer order_update_pr_initializer;
  const uint8_t o_id_key_pr_offset;
  const uint8_t o_d_id_key_pr_offset;
  const uint8_t o_w_id_key_pr_offset;

  catalog::col_oid_t ol_amount_oid;
  catalog::col_oid_t ol_delivery_d_oid;
  catalog::indexkeycol_oid_t ol_o_id_key_oid;
  catalog::indexkeycol_oid_t ol_d_id_key_oid;
  catalog::indexkeycol_oid_t ol_w_id_key_oid;
  catalog::indexkeycol_oid_t ol_number_key_oid;

  const storage::ProjectedRowInitializer order_line_pr_initializer;
  const storage::ProjectionMap order_line_pr_map;
  const uint8_t ol_amount_pr_offset;
  const uint8_t ol_delivery_d_pr_offset;
  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;

  catalog::col_oid_t c_balance_oid;
  catalog::col_oid_t c_delivery_cnt_oid;
  catalog::indexkeycol_oid_t c_id_key_oid;
  catalog::indexkeycol_oid_t c_d_id_key_oid;
  catalog::indexkeycol_oid_t c_w_id_key_oid;

  const storage::ProjectedRowInitializer customer_pr_initializer;
  const storage::ProjectionMap customer_pr_map;
  const uint8_t c_balance_pr_offset;
  const uint8_t c_delivery_cnt_pr_offset;
  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;

 public:
  explicit Delivery(const Database *const db)
      : no_o_id_key_oid(db->new_order_key_schema_.at(2).GetOid()),
        no_d_id_key_oid(db->new_order_key_schema_.at(1).GetOid()),
        no_w_id_key_oid(db->new_order_key_schema_.at(0).GetOid()),

        new_order_pr_initializer(
            db->new_order_table_->InitializerForProjectedRow({db->new_order_schema_.GetColumn(0).GetOid()}).first),
        no_o_id_key_pr_offset(static_cast<uint8_t>(db->new_order_index_->GetKeyOidToOffsetMap().at(no_o_id_key_oid))),
        no_d_id_key_pr_offset(static_cast<uint8_t>(db->new_order_index_->GetKeyOidToOffsetMap().at(no_d_id_key_oid))),
        no_w_id_key_pr_offset(static_cast<uint8_t>(db->new_order_index_->GetKeyOidToOffsetMap().at(no_w_id_key_oid))),

        o_id_key_oid(db->order_key_schema_.at(2).GetOid()),
        o_d_id_key_oid(db->order_key_schema_.at(1).GetOid()),
        o_w_id_key_oid(db->order_key_schema_.at(0).GetOid()),

        order_select_pr_initializer(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(3).GetOid()}).first),
        order_update_pr_initializer(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(5).GetOid()}).first),
        o_id_key_pr_offset(static_cast<uint8_t>(db->order_index_->GetKeyOidToOffsetMap().at(o_id_key_oid))),
        o_d_id_key_pr_offset(static_cast<uint8_t>(db->order_index_->GetKeyOidToOffsetMap().at(o_d_id_key_oid))),
        o_w_id_key_pr_offset(static_cast<uint8_t>(db->order_index_->GetKeyOidToOffsetMap().at(o_w_id_key_oid))),

        ol_amount_oid(db->order_line_schema_.GetColumn(8).GetOid()),
        ol_delivery_d_oid(db->order_line_schema_.GetColumn(6).GetOid()),
        ol_o_id_key_oid(db->order_line_key_schema_.at(2).GetOid()),
        ol_d_id_key_oid(db->order_line_key_schema_.at(1).GetOid()),
        ol_w_id_key_oid(db->order_line_key_schema_.at(0).GetOid()),
        ol_number_key_oid(db->order_line_key_schema_.at(3).GetOid()),

        order_line_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow({ol_amount_oid, ol_delivery_d_oid}).first),
        order_line_pr_map(db->order_line_table_->InitializerForProjectedRow({ol_amount_oid, ol_delivery_d_oid}).second),
        ol_amount_pr_offset(static_cast<uint8_t>(order_line_pr_map.at(ol_amount_oid))),
        ol_delivery_d_pr_offset(static_cast<uint8_t>(order_line_pr_map.at(ol_delivery_d_oid))),
        ol_o_id_key_pr_offset(static_cast<uint8_t>(db->order_line_index_->GetKeyOidToOffsetMap().at(ol_o_id_key_oid))),
        ol_d_id_key_pr_offset(static_cast<uint8_t>(db->order_line_index_->GetKeyOidToOffsetMap().at(ol_d_id_key_oid))),
        ol_w_id_key_pr_offset(static_cast<uint8_t>(db->order_line_index_->GetKeyOidToOffsetMap().at(ol_w_id_key_oid))),
        ol_number_key_pr_offset(
            static_cast<uint8_t>(db->order_line_index_->GetKeyOidToOffsetMap().at(ol_number_key_oid))),

        c_balance_oid(db->customer_schema_.GetColumn(16).GetOid()),
        c_delivery_cnt_oid(db->customer_schema_.GetColumn(19).GetOid()),
        c_id_key_oid(db->customer_key_schema_.at(2).GetOid()),
        c_d_id_key_oid(db->customer_key_schema_.at(1).GetOid()),
        c_w_id_key_oid(db->customer_key_schema_.at(0).GetOid()),

        customer_pr_initializer(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_delivery_cnt_oid}).first),
        customer_pr_map(db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_delivery_cnt_oid}).second),
        c_balance_pr_offset(static_cast<uint8_t>(customer_pr_map.at(c_balance_oid))),
        c_delivery_cnt_pr_offset(static_cast<uint8_t>(customer_pr_map.at(c_delivery_cnt_oid))),
        c_id_key_pr_offset(static_cast<uint8_t>(db->customer_index_->GetKeyOidToOffsetMap().at(c_id_key_oid))),
        c_d_id_key_pr_offset(static_cast<uint8_t>(db->customer_index_->GetKeyOidToOffsetMap().at(c_d_id_key_oid))),
        c_w_id_key_pr_offset(static_cast<uint8_t>(db->customer_index_->GetKeyOidToOffsetMap().at(c_w_id_key_oid)))

  {}

  // 2.4.2
  template <class Random>
  bool Execute(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
               Worker *const worker, const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::Delivery, "Wrong transaction type.");

    auto *const txn = txn_manager->BeginTransaction();

    for (int32_t d_id = 1; d_id <= 10; d_id++) {
      std::vector<storage::TupleSlot> index_scan_results;

      // TODO(WAN): this should be ScanLimit() or something similar, just prototyping what needs to happen
      // Look up NO_W_ID and NO_D_ID, find the lowest NO_O_ID value
      const auto new_order_key_pr_initializer = db->new_order_index_->GetProjectedRowInitializer();
      auto *const new_order_key_lo = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer);
      auto *const new_order_key_hi = new_order_key_pr_initializer.InitializeRow(worker->new_order_tuple_buffer);

      *reinterpret_cast<int32_t *>(new_order_key_lo->AccessForceNotNull(no_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(new_order_key_lo->AccessForceNotNull(no_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(new_order_key_lo->AccessForceNotNull(no_o_id_key_pr_offset)) = 1;

      *reinterpret_cast<int32_t *>(new_order_key_hi->AccessForceNotNull(no_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(new_order_key_hi->AccessForceNotNull(no_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(new_order_key_hi->AccessForceNotNull(no_o_id_key_pr_offset)) = 10000000;  // max O_ID

      db->new_order_index_->Scan(*new_order_key_lo, *new_order_key_hi, &index_scan_results);
      // If no matching row is found, then the delivery is skipped
      if (index_scan_results.empty()) {
        continue;
      }
      // Otherwise, the lowest NO_O_ID is selected
      auto *const new_order_select_tuple = new_order_pr_initializer.InitializeRow(worker->new_order_tuple_buffer);
      const auto new_order_slot = index_scan_results[0];
      bool select_result UNUSED_ATTRIBUTE = db->new_order_table_->Select(txn, new_order_slot, new_order_select_tuple);
      TERRIER_ASSERT(select_result,
                     "New Order select failed. This assertion assumes 1:1 mapping between warehouse and workers and "
                     "that indexes are getting cleaned.");
      const auto no_o_id = *reinterpret_cast<int32_t *>(new_order_select_tuple->AccessWithNullCheck(0));

      // Delete the corresponding new order table row
      bool delete_result UNUSED_ATTRIBUTE = db->new_order_table_->Delete(txn, new_order_slot);
      TERRIER_ASSERT(delete_result,
                     "New Order delete failed. This assertion assumes 1:1 mapping between warehouse and workers.");

      // Delete the index entry
      auto *const new_order_delete_key = new_order_key_pr_initializer.InitializeRow(worker->new_order_key_buffer);

      *reinterpret_cast<int32_t *>(new_order_delete_key->AccessForceNotNull(no_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(new_order_delete_key->AccessForceNotNull(no_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(new_order_delete_key->AccessForceNotNull(no_o_id_key_pr_offset)) = no_o_id;
      delete_result = db->new_order_index_->Delete(*new_order_delete_key, new_order_slot);
      TERRIER_ASSERT(
          delete_result,
          "New Order index delete failed. This assertion assumes 1:1 mapping between warehouse and workers.");

      // Look up O_W_ID, O_D_ID and O_ID
      const auto order_key_pr_initializer = db->order_index_->GetProjectedRowInitializer();
      auto *const order_key = order_key_pr_initializer.InitializeRow(worker->order_key_buffer);

      *reinterpret_cast<int32_t *>(order_key->AccessForceNotNull(o_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(order_key->AccessForceNotNull(o_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(order_key->AccessForceNotNull(o_id_key_pr_offset)) = no_o_id;

      index_scan_results.clear();
      db->order_index_->ScanKey(*order_key, &index_scan_results);
      TERRIER_ASSERT(index_scan_results.size() == 1, "Order index lookup failed.");

      // Retrieve O_C_ID
      auto &order_slot = index_scan_results[0];
      auto *order_select_tuple = order_select_pr_initializer.InitializeRow(worker->order_tuple_buffer);
      select_result = db->order_table_->Select(txn, order_slot, order_select_tuple);
      TERRIER_ASSERT(select_result,
                     "Order select failed. This assertion assumes 1:1 mapping between warehouse and workers.");

      const auto o_c_id = *reinterpret_cast<int32_t *>(order_select_tuple->AccessWithNullCheck(0));

      // update O_CARRIER_ID
      auto *order_update_tuple = order_update_pr_initializer.InitializeRow(worker->order_tuple_buffer);
      *reinterpret_cast<int32_t *>(order_update_tuple->AccessForceNotNull(0)) = args.o_carrier_id;
      const bool update_result UNUSED_ATTRIBUTE = db->order_table_->Update(txn, order_slot, *order_update_tuple);
      TERRIER_ASSERT(select_result,
                     "Order update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

      // Look up OL_W_ID, OL_D_ID, OL_O_ID
      const auto order_line_key_pr_initializer = db->order_line_index_->GetProjectedRowInitializer();
      auto *const order_line_key_lo = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer);
      auto *const order_line_key_hi = order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);

      *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_o_id_key_pr_offset)) = no_o_id;
      *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_number_key_pr_offset)) = 1;

      *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_d_id_key_pr_offset)) = d_id;
      *reinterpret_cast<int32_t *>(order_line_key_lo->AccessForceNotNull(ol_o_id_key_pr_offset)) = no_o_id;
      *reinterpret_cast<int32_t *>(order_line_key_hi->AccessForceNotNull(ol_number_key_pr_offset)) =
          15;  // max OL_NUMBER

      // Retrieve sum of all OL_AMOUNT, update every OL_DELIVERY_D to current system time
      index_scan_results.clear();
      db->order_line_index_->Scan(*order_line_key_lo, *order_line_key_hi, &index_scan_results);
      TERRIER_ASSERT(!index_scan_results.empty(), "Order line index scan failed.");

      auto *order_line_select_tuple = order_line_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);

      double ol_amount = 0.0;
      for (const auto &tuple_slot : index_scan_results) {
        db->order_line_table_->Select(txn, tuple_slot, order_line_select_tuple);
        // TODO(Matt): can't assume these Selects are succeeding because of aborts polluting indexes
        ol_amount += *reinterpret_cast<double *>(order_line_select_tuple->AccessForceNotNull(ol_amount_pr_offset));
        *reinterpret_cast<uint64_t *>(order_line_select_tuple->AccessForceNotNull(ol_delivery_d_pr_offset)) =
            args.ol_delivery_d;
      }

      // Look up C_W_ID, C_D_ID, C_ID
      const auto customer_key_pr_initializer = db->customer_index_->GetProjectedRowInitializer();
      auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer);

      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset)) = args.w_id;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset)) = o_c_id;

      // Increase C_BALANCE by OL_AMOUNT, increase C_DELIVERY_CNT
      index_scan_results.clear();
      db->customer_index_->ScanKey(*customer_key, &index_scan_results);
      TERRIER_ASSERT(!index_scan_results.empty(), "Customer index scan failed.");

      auto *const customer_select_tuple = customer_pr_initializer.InitializeRow(worker->customer_tuple_buffer);

      for (const auto &tuple_slot : index_scan_results) {
        // TODO(WAN): SelectForUpdate would be nice
        db->customer_table_->Select(txn, tuple_slot, customer_select_tuple);
        *reinterpret_cast<double *>(customer_select_tuple->AccessForceNotNull(c_balance_pr_offset)) += ol_amount;
        (*reinterpret_cast<int16_t *>(customer_select_tuple->AccessForceNotNull(c_delivery_cnt_pr_offset)))++;
        db->customer_table_->Update(txn, tuple_slot, *customer_select_tuple);
      }
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    return true;
  }
};

}  // namespace terrier::tpcc
