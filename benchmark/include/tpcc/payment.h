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

  const uint8_t d_id_key_pr_offset;
  const uint8_t d_w_id_key_pr_offset;

  const catalog::col_oid_t d_name_oid;
  const catalog::col_oid_t d_street_1_oid;
  const catalog::col_oid_t d_street_2_oid;
  const catalog::col_oid_t d_city_oid;
  const catalog::col_oid_t d_state_oid;
  const catalog::col_oid_t d_zip_oid;
  const catalog::col_oid_t d_ytd_oid;
  const storage::ProjectedRowInitializer district_select_pr_initializer;
  const storage::ProjectionMap district_select_pr_map;
  const uint8_t d_name_select_pr_offset;
  const uint8_t d_street_1_select_pr_offset;
  const uint8_t d_street_2_select_pr_offset;
  const uint8_t d_city_select_pr_offset;
  const uint8_t d_state_select_pr_offset;
  const uint8_t d_zip_select_pr_offset;
  const uint8_t d_ytd_select_pr_offset;
  const storage::ProjectedRowInitializer district_update_pr_initializer;

  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;
  const uint8_t c_id_name_key_pr_offset;
  const uint8_t c_d_id_name_key_pr_offset;
  const uint8_t c_w_id_name_key_pr_offset;

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
        warehouse_update_pr_initializer(db->warehouse_table_->InitializerForProjectedRow({w_ytd_oid}).first),

        // District metadata
        d_id_key_pr_offset(db->district_index_->GetKeyOidToOffsetMap().at(db->district_key_schema_.at(1).GetOid())),
        d_w_id_key_pr_offset(db->district_index_->GetKeyOidToOffsetMap().at(db->district_key_schema_.at(0).GetOid())),

        d_name_oid(db->district_schema_.GetColumn(2).GetOid()),
        d_street_1_oid(db->district_schema_.GetColumn(3).GetOid()),
        d_street_2_oid(db->district_schema_.GetColumn(4).GetOid()),
        d_city_oid(db->district_schema_.GetColumn(5).GetOid()),
        d_state_oid(db->district_schema_.GetColumn(6).GetOid()),
        d_zip_oid(db->district_schema_.GetColumn(7).GetOid()),
        d_ytd_oid(db->district_schema_.GetColumn(9).GetOid()),
        district_select_pr_initializer(db->district_table_
                                           ->InitializerForProjectedRow({d_name_oid, d_street_1_oid, d_street_2_oid,
                                                                         d_city_oid, d_state_oid, d_zip_oid, d_ytd_oid})
                                           .first),
        district_select_pr_map(db->district_table_
                                   ->InitializerForProjectedRow({d_name_oid, d_street_1_oid, d_street_2_oid, d_city_oid,
                                                                 d_state_oid, d_zip_oid, d_ytd_oid})
                                   .second),
        d_name_select_pr_offset(district_select_pr_map.at(d_name_oid)),
        d_street_1_select_pr_offset(district_select_pr_map.at(d_street_1_oid)),
        d_street_2_select_pr_offset(district_select_pr_map.at(d_street_2_oid)),
        d_city_select_pr_offset(district_select_pr_map.at(d_city_oid)),
        d_state_select_pr_offset(district_select_pr_map.at(d_state_oid)),
        d_zip_select_pr_offset(district_select_pr_map.at(d_zip_oid)),
        d_ytd_select_pr_offset(district_select_pr_map.at(d_ytd_oid)),
        district_update_pr_initializer(db->district_table_->InitializerForProjectedRow({d_ytd_oid}).first),

        // Customer metadata
        c_id_key_pr_offset(db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(2).GetOid())),
        c_d_id_key_pr_offset(db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(1).GetOid())),
        c_w_id_key_pr_offset(db->customer_index_->GetKeyOidToOffsetMap().at(db->customer_key_schema_.at(0).GetOid())),
        c_id_name_key_pr_offset(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(2).GetOid())),
        c_d_id_name_key_pr_offset(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(1).GetOid())),
        c_w_id_name_key_pr_offset(
            db->customer_name_index_->GetKeyOidToOffsetMap().at(db->customer_name_key_schema_.at(0).GetOid()))

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

    // Look up D_ID, W_ID in index
    const auto district_key_pr_initializer = db->district_index_->GetProjectedRowInitializer();
    auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer);

    *reinterpret_cast<int32_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int32_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset)) = args.w_id;

    index_scan_results.clear();
    db->district_index_->ScanKey(*district_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

    // Select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD in table
    auto *const district_select_tuple = district_select_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    db->district_table_->Select(txn, index_scan_results[0], district_select_tuple);
    const auto UNUSED_ATTRIBUTE d_name =
        *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_name_select_pr_offset));
    const auto UNUSED_ATTRIBUTE d_street_1 = *reinterpret_cast<storage::VarlenEntry *>(
        district_select_tuple->AccessWithNullCheck(d_street_1_select_pr_offset));
    const auto UNUSED_ATTRIBUTE d_street_2 = *reinterpret_cast<storage::VarlenEntry *>(
        district_select_tuple->AccessWithNullCheck(d_street_2_select_pr_offset));
    const auto UNUSED_ATTRIBUTE d_city =
        *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_city_select_pr_offset));
    const auto UNUSED_ATTRIBUTE d_state =
        *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_state_select_pr_offset));
    const auto UNUSED_ATTRIBUTE d_zip =
        *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_zip_select_pr_offset));
    const auto d_ytd = *reinterpret_cast<double *>(district_select_tuple->AccessWithNullCheck(d_ytd_select_pr_offset));

    // Increase D_YTD by H_AMOUNT in table
    auto *const district_update_tuple = district_update_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    *reinterpret_cast<double *>(district_update_tuple->AccessForceNotNull(0)) = d_ytd + args.h_amount;

    result = db->district_table_->Update(txn, index_scan_results[0], *district_update_tuple);
    TERRIER_ASSERT(result, "District update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

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
    } else {
      // Look up C_NAME, D_ID, W_ID in index
      const auto customer_name_key_pr_initializer = db->customer_name_index_->GetProjectedRowInitializer();
      auto *const customer_key = customer_name_key_pr_initializer.InitializeRow(worker->customer_name_key_buffer);

      *reinterpret_cast<storage::VarlenEntry *>(customer_key->AccessForceNotNull(c_id_name_key_pr_offset)) =
          args.c_last;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_d_id_name_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_w_id_name_key_pr_offset)) = args.w_id;

      index_scan_results.clear();
      db->customer_index_->ScanKey(*customer_key, &index_scan_results);
      TERRIER_ASSERT(!index_scan_results.empty(), "Customer Name index lookup failed.");
    }

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
