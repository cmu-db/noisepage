#pragma once

#include <string_view>
#include "../../../src/include/catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "tpcc/database.h"
#include "tpcc/util.h"
#include "tpcc/workload.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

class Payment {
 private:
  const catalog::col_oid_t w_name_oid;
  const catalog::col_oid_t w_street_1_oid;
  const catalog::col_oid_t w_street_2_oid;
  const catalog::col_oid_t w_city_oid;
  const catalog::col_oid_t w_state_oid;
  const catalog::col_oid_t w_zip_oid;
  const catalog::col_oid_t w_ytd_oid;
  const storage::ProjectedRowInitializer warehouse_select_pr_initializer;
  const storage::ProjectionMap warehouse_select_pr_map;
  const uint8_t w_name_select_pr_offset;
  const uint8_t w_street_1_select_pr_offset;
  const uint8_t w_street_2_select_pr_offset;
  const uint8_t w_city_select_pr_offset;
  const uint8_t w_state_select_pr_offset;
  const uint8_t w_zip_select_pr_offset;
  const uint8_t w_ytd_select_pr_offset;
  const storage::ProjectedRowInitializer warehouse_update_pr_initializer;

 public:
  explicit Payment(const Database *const db)

      :  // Warehouse metadata
        w_name_oid(db->warehouse_schema_.GetColumn(1).GetOid()),
        w_street_1_oid(db->warehouse_schema_.GetColumn(2).GetOid()),
        w_street_2_oid(db->warehouse_schema_.GetColumn(3).GetOid()),
        w_city_oid(db->warehouse_schema_.GetColumn(4).GetOid()),
        w_state_oid(db->warehouse_schema_.GetColumn(5).GetOid()),
        w_zip_oid(db->warehouse_schema_.GetColumn(6).GetOid()),
        w_ytd_oid(db->warehouse_schema_.GetColumn(8).GetOid()),
        warehouse_select_pr_initializer(
            db->warehouse_table_
                ->InitializerForProjectedRow(
                    {w_name_oid, w_street_1_oid, w_street_2_oid, w_city_oid, w_state_oid, w_zip_oid, w_ytd_oid})
                .first),
        warehouse_select_pr_map(db->warehouse_table_
                                    ->InitializerForProjectedRow({w_name_oid, w_street_1_oid, w_street_2_oid,
                                                                  w_city_oid, w_state_oid, w_zip_oid, w_ytd_oid})
                                    .second),
        w_name_select_pr_offset(warehouse_select_pr_map.at(w_name_oid)),
        w_street_1_select_pr_offset(warehouse_select_pr_map.at(w_street_1_oid)),
        w_street_2_select_pr_offset(warehouse_select_pr_map.at(w_street_2_oid)),
        w_city_select_pr_offset(warehouse_select_pr_map.at(w_city_oid)),
        w_state_select_pr_offset(warehouse_select_pr_map.at(w_state_oid)),
        w_zip_select_pr_offset(warehouse_select_pr_map.at(w_zip_oid)),
        w_ytd_select_pr_offset(warehouse_select_pr_map.at(w_ytd_oid)),
        warehouse_update_pr_initializer(db->warehouse_table_->InitializerForProjectedRow({w_ytd_oid}).first)

  {}

  // 2.4.2
  template <class Random>
  bool Execute(transaction::TransactionManager *const txn_manager, Random *const generator, Database *const db,
               Worker *const worker, const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::Payment, "Wrong transaction type.");

    auto *const txn = txn_manager->BeginTransaction();

    // Look up W_ID in index
    const auto warehouse_key_pr_initializer = db->warehouse_index_->GetProjectedRowInitializer();
    auto *const warehouse_key = warehouse_key_pr_initializer.InitializeRow(worker->warehouse_key_buffer);

    *reinterpret_cast<int32_t *>(warehouse_key->AccessForceNotNull(0)) = args.w_id;

    std::vector<storage::TupleSlot> index_scan_results;
    db->warehouse_index_->ScanKey(*warehouse_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

    // Select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD in table
    auto *const warehouse_select_tuple = warehouse_select_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    db->warehouse_table_->Select(txn, index_scan_results[0], warehouse_select_tuple);
    const auto UNUSED_ATTRIBUTE w_name =
        *reinterpret_cast<storage::VarlenEntry *>(warehouse_select_tuple->AccessWithNullCheck(w_name_select_pr_offset));
    const auto UNUSED_ATTRIBUTE w_street_1 = *reinterpret_cast<storage::VarlenEntry *>(
        warehouse_select_tuple->AccessWithNullCheck(w_street_1_select_pr_offset));
    const auto UNUSED_ATTRIBUTE w_street_2 = *reinterpret_cast<storage::VarlenEntry *>(
        warehouse_select_tuple->AccessWithNullCheck(w_street_2_select_pr_offset));
    const auto UNUSED_ATTRIBUTE w_city =
        *reinterpret_cast<storage::VarlenEntry *>(warehouse_select_tuple->AccessWithNullCheck(w_city_select_pr_offset));
    const auto UNUSED_ATTRIBUTE w_state = *reinterpret_cast<storage::VarlenEntry *>(
        warehouse_select_tuple->AccessWithNullCheck(w_state_select_pr_offset));
    const auto UNUSED_ATTRIBUTE w_zip =
        *reinterpret_cast<storage::VarlenEntry *>(warehouse_select_tuple->AccessWithNullCheck(w_zip_select_pr_offset));
    const auto w_ytd = *reinterpret_cast<double *>(warehouse_select_tuple->AccessWithNullCheck(w_ytd_select_pr_offset));

    // Increase W_YTD by H_AMOUNT in table
    auto *const warehouse_update_tuple = warehouse_update_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    *reinterpret_cast<double *>(warehouse_update_tuple->AccessForceNotNull(0)) = w_ytd + args.h_amount;

    bool UNUSED_ATTRIBUTE result = db->warehouse_table_->Update(txn, index_scan_results[0], *warehouse_update_tuple);
    TERRIER_ASSERT(result,
                   "Warehouse update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
