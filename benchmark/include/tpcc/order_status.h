#pragma once

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

class OrderStatus {
 private:
  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;
  const uint8_t c_last_name_key_pr_offset;
  const uint8_t c_d_id_name_key_pr_offset;
  const uint8_t c_w_id_name_key_pr_offset;
  const catalog::col_oid_t c_id_oid;
  const catalog::col_oid_t c_balance_oid;
  const catalog::col_oid_t c_first_oid;
  const catalog::col_oid_t c_middle_oid;
  const catalog::col_oid_t c_last_oid;
  const storage::ProjectedRowInitializer c_first_pr_initializer;
  const storage::ProjectedRowInitializer customer_select_pr_initializer;
  const storage::ProjectionMap customer_select_pr_map;
  const uint8_t c_id_select_pr_offset;
  const uint8_t c_balance_select_pr_offset;
  const uint8_t c_first_select_pr_offset;
  const uint8_t c_middle_select_pr_offset;
  const uint8_t c_last_select_pr_offset;

  const uint8_t o_id_secondary_key_pr_offset;
  const uint8_t o_d_id_secondary_key_pr_offset;
  const uint8_t o_w_id_secondary_key_pr_offset;
  const uint8_t o_c_id_secondary_key_pr_offset;
  const catalog::col_oid_t o_id_oid;
  const catalog::col_oid_t o_entry_d_oid;
  const catalog::col_oid_t o_carrier_id_oid;
  const storage::ProjectedRowInitializer order_select_pr_initializer;
  const storage::ProjectionMap order_select_pr_map;
  const uint8_t o_id_select_pr_offset;

  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;
  const catalog::col_oid_t ol_i_id_oid;
  const catalog::col_oid_t ol_supply_w_id_oid;
  const catalog::col_oid_t ol_quantity_oid;
  const catalog::col_oid_t ol_amount_oid;
  const catalog::col_oid_t ol_delivery_d_oid;
  const storage::ProjectedRowInitializer order_line_select_pr_initializer;

 public:
  explicit OrderStatus(const Database *const db)
      : c_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(2).GetOid()))),
        c_d_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(1).GetOid()))),
        c_w_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(0).GetOid()))),
        c_last_name_key_pr_offset(static_cast<uint8_t>(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(2).GetOid()))),
        c_d_id_name_key_pr_offset(static_cast<uint8_t>(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(1).GetOid()))),
        c_w_id_name_key_pr_offset(static_cast<uint8_t>(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(0).GetOid()))),

        c_id_oid(db->customer_schema_.GetColumn(0).GetOid()),
        c_balance_oid(db->customer_schema_.GetColumn(16).GetOid()),
        c_first_oid(db->customer_schema_.GetColumn(3).GetOid()),
        c_middle_oid(db->customer_schema_.GetColumn(4).GetOid()),
        c_last_oid(db->customer_schema_.GetColumn(5).GetOid()),

        c_first_pr_initializer(db->customer_table_->InitializerForProjectedRow({c_first_oid}).first),
        customer_select_pr_initializer(
            db->customer_table_
                ->InitializerForProjectedRow({c_id_oid, c_balance_oid, c_first_oid, c_middle_oid, c_last_oid})
                .first),
        customer_select_pr_map(
            db->customer_table_
                ->InitializerForProjectedRow({c_id_oid, c_balance_oid, c_first_oid, c_middle_oid, c_last_oid})
                .second),

        c_id_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_id_oid))),
        c_balance_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_balance_oid))),
        c_first_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_first_oid))),
        c_middle_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_middle_oid))),
        c_last_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_last_oid))),
        o_id_secondary_key_pr_offset(static_cast<uint8_t>(
            db->order_secondary_index_->GetKeyOidToOffsetMap().at(db->order_secondary_key_schema_.at(3).GetOid()))),
        o_d_id_secondary_key_pr_offset(static_cast<uint8_t>(
            db->order_secondary_index_->GetKeyOidToOffsetMap().at(db->order_secondary_key_schema_.at(1).GetOid()))),
        o_w_id_secondary_key_pr_offset(static_cast<uint8_t>(
            db->order_secondary_index_->GetKeyOidToOffsetMap().at(db->order_secondary_key_schema_.at(0).GetOid()))),
        o_c_id_secondary_key_pr_offset(static_cast<uint8_t>(
            db->order_secondary_index_->GetKeyOidToOffsetMap().at(db->order_secondary_key_schema_.at(2).GetOid()))),
        o_id_oid(db->order_schema_.GetColumn(0).GetOid()),
        o_entry_d_oid(db->order_schema_.GetColumn(4).GetOid()),
        o_carrier_id_oid(db->order_schema_.GetColumn(5).GetOid()),
        order_select_pr_initializer(
            db->order_table_->InitializerForProjectedRow({o_id_oid, o_entry_d_oid, o_carrier_id_oid}).first),
        order_select_pr_map(
            db->order_table_->InitializerForProjectedRow({o_id_oid, o_entry_d_oid, o_carrier_id_oid}).second),
        o_id_select_pr_offset(static_cast<uint8_t>(order_select_pr_map.at(o_id_oid))),
        ol_o_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(2).GetOid()))),
        ol_d_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(1).GetOid()))),
        ol_w_id_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(0).GetOid()))),
        ol_number_key_pr_offset(static_cast<uint8_t>(
            db->order_line_index_->GetKeyOidToOffsetMap().at(db->order_line_key_schema_.at(3).GetOid()))),

        ol_i_id_oid(db->order_line_schema_.GetColumn(4).GetOid()),
        ol_supply_w_id_oid(db->order_line_schema_.GetColumn(5).GetOid()),
        ol_quantity_oid(db->order_line_schema_.GetColumn(7).GetOid()),
        ol_amount_oid(db->order_line_schema_.GetColumn(8).GetOid()),
        ol_delivery_d_oid(db->order_line_schema_.GetColumn(6).GetOid()),
        order_line_select_pr_initializer(
            db->order_line_table_
                ->InitializerForProjectedRow(
                    {ol_i_id_oid, ol_supply_w_id_oid, ol_quantity_oid, ol_amount_oid, ol_delivery_d_oid})
                .first)

  {}

  // 2.4.2
  template <class Random>
  bool Execute(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
               Worker *const worker, const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::OrderStatus, "Wrong transaction type.");

    auto *const txn = txn_manager->BeginTransaction();

    storage::TupleSlot customer_slot;
    std::vector<storage::TupleSlot> index_scan_results;
    if (!args.use_c_last) {
      // Look up C_ID, D_ID, W_ID in index
      const auto customer_key_pr_initializer = db->customer_index_->GetProjectedRowInitializer();
      auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer);

      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset)) = args.c_id;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset)) = args.w_id;

      index_scan_results.clear();
      db->customer_index_->ScanKey(*customer_key, &index_scan_results);
      TERRIER_ASSERT(index_scan_results.size() == 1, "Customer index lookup failed.");
      customer_slot = index_scan_results[0];
    } else {
      // Look up C_LAST, D_ID, W_ID in index
      const auto customer_name_key_pr_initializer = db->customer_name_index_->GetProjectedRowInitializer();
      auto *const customer_name_key = customer_name_key_pr_initializer.InitializeRow(worker->customer_name_key_buffer);

      *reinterpret_cast<storage::VarlenEntry *>(customer_name_key->AccessForceNotNull(c_last_name_key_pr_offset)) =
          args.c_last;
      *reinterpret_cast<int32_t *>(customer_name_key->AccessForceNotNull(c_d_id_name_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int32_t *>(customer_name_key->AccessForceNotNull(c_w_id_name_key_pr_offset)) = args.w_id;

      index_scan_results.clear();
      db->customer_name_index_->ScanKey(*customer_name_key, &index_scan_results);
      TERRIER_ASSERT(!index_scan_results.empty(), "Customer Name index lookup failed.");

      if (index_scan_results.size() > 1) {
        std::map<std::string, storage::TupleSlot> sorted_index_scan_results;
        for (const auto &tuple_slot : index_scan_results) {
          auto *const c_first_select_tuple = c_first_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
          bool UNUSED_ATTRIBUTE select_result = db->customer_table_->Select(txn, tuple_slot, c_first_select_tuple);
          TERRIER_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");
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
    auto *const customer_select_tuple = customer_select_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
    bool select_result = db->customer_table_->Select(txn, customer_slot, customer_select_tuple);
    TERRIER_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");

    const auto UNUSED_ATTRIBUTE c_id =
        !args.use_c_last
            ? args.c_id
            : *reinterpret_cast<int32_t *>(customer_select_tuple->AccessWithNullCheck(c_id_select_pr_offset));

    // look up in secondary Order index
    const auto order_secondary_key_pr_initializer = db->order_secondary_index_->GetProjectedRowInitializer();
    auto *const order_secondary_low_key =
        order_secondary_key_pr_initializer.InitializeRow(worker->order_secondary_key_buffer);
    auto *const order_secondary_high_key =
        order_secondary_key_pr_initializer.InitializeRow(worker->order_tuple_buffer);  // it's large enough

    *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_id_secondary_key_pr_offset)) = 0;
    *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_d_id_secondary_key_pr_offset)) =
        args.d_id;
    *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_w_id_secondary_key_pr_offset)) =
        args.w_id;
    *reinterpret_cast<int32_t *>(order_secondary_low_key->AccessForceNotNull(o_c_id_secondary_key_pr_offset)) =
        args.c_id;

    *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_id_secondary_key_pr_offset)) =
        INT32_MAX;
    *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_d_id_secondary_key_pr_offset)) =
        args.d_id;
    *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_w_id_secondary_key_pr_offset)) =
        args.w_id;
    *reinterpret_cast<int32_t *>(order_secondary_high_key->AccessForceNotNull(o_c_id_secondary_key_pr_offset)) =
        args.c_id;

    index_scan_results.clear();
    db->order_secondary_index_->Scan(*order_secondary_low_key, *order_secondary_high_key, &index_scan_results);

    // Select O_ID, O_ENTRY_D, O_CARRIER_ID from table
    auto *const order_select_tuple = order_select_pr_initializer.InitializeRow(worker->order_tuple_buffer);
    for (auto reverse_it = index_scan_results.rbegin(); reverse_it != index_scan_results.rend(); reverse_it++) {
      select_result = db->order_table_->Select(txn, *reverse_it, order_select_tuple);
      if (select_result) break;  // It can fail if the index scan results in uncommitted TupleSlots
    }

    const auto o_id = *reinterpret_cast<int32_t *>(order_select_tuple->AccessWithNullCheck(o_id_select_pr_offset));

    // look up in Order Line index
    const auto order_line_key_pr_initializer = db->order_line_index_->GetProjectedRowInitializer();
    auto *const order_line_low_key = order_line_key_pr_initializer.InitializeRow(worker->order_line_key_buffer);
    auto *const order_line_high_key =
        order_line_key_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);  // it's large enough

    *reinterpret_cast<int32_t *>(order_line_low_key->AccessForceNotNull(ol_number_key_pr_offset)) = 1;
    *reinterpret_cast<int32_t *>(order_line_low_key->AccessForceNotNull(ol_d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(order_line_low_key->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
    *reinterpret_cast<int32_t *>(order_line_low_key->AccessForceNotNull(ol_o_id_key_pr_offset)) = o_id;

    *reinterpret_cast<int32_t *>(order_line_high_key->AccessForceNotNull(ol_number_key_pr_offset)) = 15;
    *reinterpret_cast<int32_t *>(order_line_high_key->AccessForceNotNull(ol_d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(order_line_high_key->AccessForceNotNull(ol_w_id_key_pr_offset)) = args.w_id;
    *reinterpret_cast<int32_t *>(order_line_high_key->AccessForceNotNull(ol_o_id_key_pr_offset)) = o_id;

    index_scan_results.clear();
    db->order_line_index_->Scan(*order_line_low_key, *order_line_high_key, &index_scan_results);

    TERRIER_ASSERT(!index_scan_results.empty() && index_scan_results.size() <= 15,
                   "There should be at least 1 Order Line item, but no more than 15.");

    // Select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D for every result of the index scan
    auto *const order_line_select_tuple =
        order_line_select_pr_initializer.InitializeRow(worker->order_line_tuple_buffer);
    for (const auto &tuple_slot : index_scan_results) {
      select_result = db->order_line_table_->Select(txn, tuple_slot, order_line_select_tuple);
      TERRIER_ASSERT(select_result,
                     "We already confirmed that this is a committed order above, so none of these should fail.");
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
