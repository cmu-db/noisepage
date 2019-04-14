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
            db->order_secondary_index_->GetKeyOidToOffsetMap().at(db->order_secondary_key_schema_.at(2).GetOid()))) {}

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
          db->customer_table_->Select(txn, tuple_slot, c_first_select_tuple);
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
    db->customer_table_->Select(txn, customer_slot, customer_select_tuple);

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

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
