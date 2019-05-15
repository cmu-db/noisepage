#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include "catalog/catalog_defs.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "util/tpcc/database.h"
#include "util/tpcc/util.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"
#include "util/transaction_benchmark_util.h"

/**
 * Delivery transaction according to section 2.5.2 of the specification
 */
namespace terrier::tpcc {

class Payment {
 private:
  // Warehouse metadata
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
  const uint8_t w_ytd_select_pr_offset;
  const storage::ProjectedRowInitializer warehouse_update_pr_initializer;

  // District metadata
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
  const uint8_t d_ytd_select_pr_offset;
  const storage::ProjectedRowInitializer district_update_pr_initializer;

  // Customer metadata
  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;
  const uint8_t c_last_name_key_pr_offset;
  const uint8_t c_d_id_name_key_pr_offset;
  const uint8_t c_w_id_name_key_pr_offset;
  const storage::ProjectedRowInitializer c_first_pr_initializer;
  const storage::ProjectedRowInitializer customer_select_pr_initializer;
  const storage::ProjectionMap customer_select_pr_map;
  const catalog::col_oid_t c_id_oid;
  const catalog::col_oid_t c_credit_oid;
  const catalog::col_oid_t c_balance_oid;
  const catalog::col_oid_t c_ytd_payment_oid;
  const catalog::col_oid_t c_payment_cnt_oid;
  const catalog::col_oid_t c_data_oid;
  const uint8_t c_id_select_pr_offset;
  const uint8_t c_credit_select_pr_offset;
  const uint8_t c_balance_select_pr_offset;
  const uint8_t c_ytd_payment_select_pr_offset;
  const uint8_t c_payment_cnt_select_pr_offset;
  const uint8_t c_data_select_pr_offset;
  const storage::ProjectedRowInitializer customer_update_pr_initializer;
  const storage::ProjectionMap customer_update_pr_map;
  const uint8_t c_balance_update_pr_offset;
  const uint8_t c_ytd_payment_update_pr_offset;
  const uint8_t c_payment_cnt_update_pr_offset;
  const storage::ProjectedRowInitializer c_data_pr_initializer;

  // History metadata
  const storage::ProjectedRowInitializer history_insert_pr_initializer;
  const storage::ProjectionMap history_insert_pr_map;
  const uint8_t h_c_id_insert_pr_offset;
  const uint8_t h_c_d_id_insert_pr_offset;
  const uint8_t h_c_w_id_insert_pr_offset;
  const uint8_t h_d_id_insert_pr_offset;
  const uint8_t h_w_id_insert_pr_offset;
  const uint8_t h_date_insert_pr_offset;
  const uint8_t h_amount_insert_pr_offset;
  const uint8_t h_data_insert_pr_offset;

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
        w_name_select_pr_offset(static_cast<uint8_t>(warehouse_select_pr_map.at(w_name_oid))),
        w_ytd_select_pr_offset(static_cast<uint8_t>(warehouse_select_pr_map.at(w_ytd_oid))),
        warehouse_update_pr_initializer(db->warehouse_table_->InitializerForProjectedRow({w_ytd_oid}).first),

        // District metadata
        d_id_key_pr_offset(static_cast<uint8_t>(
            db->district_primary_index_->GetKeyOidToOffsetMap().at(db->district_primary_index_schema_.at(1).GetOid()))),
        d_w_id_key_pr_offset(static_cast<uint8_t>(
            db->district_primary_index_->GetKeyOidToOffsetMap().at(db->district_primary_index_schema_.at(0).GetOid()))),

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
        d_name_select_pr_offset(static_cast<uint8_t>(district_select_pr_map.at(d_name_oid))),
        d_ytd_select_pr_offset(static_cast<uint8_t>(district_select_pr_map.at(d_ytd_oid))),
        district_update_pr_initializer(db->district_table_->InitializerForProjectedRow({d_ytd_oid}).first),

        // Customer metadata
        c_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_primary_index_->GetKeyOidToOffsetMap().at(db->customer_primary_index_schema_.at(2).GetOid()))),
        c_d_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_primary_index_->GetKeyOidToOffsetMap().at(db->customer_primary_index_schema_.at(1).GetOid()))),
        c_w_id_key_pr_offset(static_cast<uint8_t>(
            db->customer_primary_index_->GetKeyOidToOffsetMap().at(db->customer_primary_index_schema_.at(0).GetOid()))),
        c_last_name_key_pr_offset(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.at(2).GetOid()))),
        c_d_id_name_key_pr_offset(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.at(1).GetOid()))),
        c_w_id_name_key_pr_offset(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.at(0).GetOid()))),
        c_first_pr_initializer(
            db->customer_table_->InitializerForProjectedRow({db->customer_schema_.GetColumn(3).GetOid()}).first),
        customer_select_pr_initializer(
            db->customer_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->customer_schema_)).first),
        customer_select_pr_map(
            db->customer_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->customer_schema_)).second),

        c_id_oid(db->customer_schema_.GetColumn(0).GetOid()),
        c_credit_oid(db->customer_schema_.GetColumn(13).GetOid()),
        c_balance_oid(db->customer_schema_.GetColumn(16).GetOid()),
        c_ytd_payment_oid(db->customer_schema_.GetColumn(17).GetOid()),
        c_payment_cnt_oid(db->customer_schema_.GetColumn(18).GetOid()),
        c_data_oid(db->customer_schema_.GetColumn(20).GetOid()),

        c_id_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_id_oid))),
        c_credit_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_credit_oid))),
        c_balance_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_balance_oid))),
        c_ytd_payment_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_ytd_payment_oid))),
        c_payment_cnt_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_payment_cnt_oid))),
        c_data_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_data_oid))),
        customer_update_pr_initializer(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_ytd_payment_oid, c_payment_cnt_oid})
                .first),
        customer_update_pr_map(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_ytd_payment_oid, c_payment_cnt_oid})
                .second),
        c_balance_update_pr_offset(static_cast<uint8_t>(customer_update_pr_map.at(c_balance_oid))),
        c_ytd_payment_update_pr_offset(static_cast<uint8_t>(customer_update_pr_map.at(c_ytd_payment_oid))),
        c_payment_cnt_update_pr_offset(static_cast<uint8_t>(customer_update_pr_map.at(c_payment_cnt_oid))),
        c_data_pr_initializer(db->customer_table_->InitializerForProjectedRow({c_data_oid}).first),

        history_insert_pr_initializer(
            db->history_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->history_schema_)).first),
        history_insert_pr_map(
            db->history_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->history_schema_)).second),

        h_c_id_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(0).GetOid()))),
        h_c_d_id_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(1).GetOid()))),
        h_c_w_id_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(2).GetOid()))),
        h_d_id_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(3).GetOid()))),
        h_w_id_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(4).GetOid()))),
        h_date_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(5).GetOid()))),
        h_amount_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(6).GetOid()))),
        h_data_insert_pr_offset(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(7).GetOid())))

  {}

  // 2.4.2
  bool Execute(transaction::TransactionManager *const txn_manager, Database *const db, Worker *const worker,
               const TransactionArgs &args) const {
    TERRIER_ASSERT(args.type == TransactionType::Payment, "Wrong transaction type.");

    auto *const txn = txn_manager->BeginTransaction();

    // Look up W_ID in index
    const auto warehouse_key_pr_initializer = db->warehouse_primary_index_->GetProjectedRowInitializer();
    auto *const warehouse_key = warehouse_key_pr_initializer.InitializeRow(worker->warehouse_key_buffer);

    *reinterpret_cast<int8_t *>(warehouse_key->AccessForceNotNull(0)) = args.w_id;

    std::vector<storage::TupleSlot> index_scan_results;
    db->warehouse_primary_index_->ScanKey(*warehouse_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "Warehouse index lookup failed.");

    // Select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD in table
    auto *const warehouse_select_tuple = warehouse_select_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    bool UNUSED_ATTRIBUTE select_result =
        db->warehouse_table_->Select(txn, index_scan_results[0], warehouse_select_tuple);
    TERRIER_ASSERT(select_result, "Warehouse table doesn't change. All lookups should succeed.");
    const auto w_name =
        *reinterpret_cast<storage::VarlenEntry *>(warehouse_select_tuple->AccessWithNullCheck(w_name_select_pr_offset));
    const auto w_ytd = *reinterpret_cast<double *>(warehouse_select_tuple->AccessWithNullCheck(w_ytd_select_pr_offset));
    TERRIER_ASSERT(w_ytd >= 300000.0, "Invalid w_ytd read from the Warehouse table.");

    // Increase W_YTD by H_AMOUNT in table
    auto *const warehouse_update_tuple = warehouse_update_pr_initializer.InitializeRow(worker->warehouse_tuple_buffer);
    *reinterpret_cast<double *>(warehouse_update_tuple->AccessForceNotNull(0)) = w_ytd + args.h_amount;

    bool UNUSED_ATTRIBUTE result = db->warehouse_table_->Update(txn, index_scan_results[0], *warehouse_update_tuple);
    TERRIER_ASSERT(result,
                   "Warehouse update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    // Look up D_ID, W_ID in index
    const auto district_key_pr_initializer = db->district_primary_index_->GetProjectedRowInitializer();
    auto *const district_key = district_key_pr_initializer.InitializeRow(worker->district_key_buffer);

    *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_id_key_pr_offset)) = args.d_id;
    *reinterpret_cast<int8_t *>(district_key->AccessForceNotNull(d_w_id_key_pr_offset)) = args.w_id;

    index_scan_results.clear();
    db->district_primary_index_->ScanKey(*district_key, &index_scan_results);
    TERRIER_ASSERT(index_scan_results.size() == 1, "District index lookup failed.");

    // Select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD in table
    auto *const district_select_tuple = district_select_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    select_result = db->district_table_->Select(txn, index_scan_results[0], district_select_tuple);
    TERRIER_ASSERT(select_result, "District table doesn't change. All lookups should succeed.");
    const auto d_name =
        *reinterpret_cast<storage::VarlenEntry *>(district_select_tuple->AccessWithNullCheck(d_name_select_pr_offset));
    const auto d_ytd = *reinterpret_cast<double *>(district_select_tuple->AccessWithNullCheck(d_ytd_select_pr_offset));
    TERRIER_ASSERT(d_ytd >= 30000.0, "Invalid d_ytd read from the District table.");

    // Increase D_YTD by H_AMOUNT in table
    auto *const district_update_tuple = district_update_pr_initializer.InitializeRow(worker->district_tuple_buffer);
    *reinterpret_cast<double *>(district_update_tuple->AccessForceNotNull(0)) = d_ytd + args.h_amount;

    result = db->district_table_->Update(txn, index_scan_results[0], *district_update_tuple);
    TERRIER_ASSERT(result, "District update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    storage::TupleSlot customer_slot;
    if (!args.use_c_last) {
      // Look up C_ID, D_ID, W_ID in index
      const auto customer_key_pr_initializer = db->customer_primary_index_->GetProjectedRowInitializer();
      auto *const customer_key = customer_key_pr_initializer.InitializeRow(worker->customer_key_buffer);

      *reinterpret_cast<int32_t *>(customer_key->AccessForceNotNull(c_id_key_pr_offset)) = args.c_id;
      *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_d_id_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int8_t *>(customer_key->AccessForceNotNull(c_w_id_key_pr_offset)) = args.w_id;

      index_scan_results.clear();
      db->customer_primary_index_->ScanKey(*customer_key, &index_scan_results);
      TERRIER_ASSERT(index_scan_results.size() == 1, "Customer index lookup failed.");
      customer_slot = index_scan_results[0];
    } else {
      // Look up C_LAST, D_ID, W_ID in index
      const auto customer_name_key_pr_initializer = db->customer_secondary_index_->GetProjectedRowInitializer();
      auto *const customer_name_key = customer_name_key_pr_initializer.InitializeRow(worker->customer_name_key_buffer);

      *reinterpret_cast<storage::VarlenEntry *>(customer_name_key->AccessForceNotNull(c_last_name_key_pr_offset)) =
          args.c_last;
      *reinterpret_cast<int8_t *>(customer_name_key->AccessForceNotNull(c_d_id_name_key_pr_offset)) = args.d_id;
      *reinterpret_cast<int8_t *>(customer_name_key->AccessForceNotNull(c_w_id_name_key_pr_offset)) = args.w_id;

      index_scan_results.clear();
      db->customer_secondary_index_->ScanKey(*customer_name_key, &index_scan_results);
      TERRIER_ASSERT(!index_scan_results.empty(), "Customer Name index lookup failed.");

      if (index_scan_results.size() > 1) {
        std::map<std::string, storage::TupleSlot> sorted_index_scan_results;
        for (const auto &tuple_slot : index_scan_results) {
          auto *const c_first_select_tuple = c_first_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
          select_result = db->customer_table_->Select(txn, tuple_slot, c_first_select_tuple);
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
    select_result = db->customer_table_->Select(txn, customer_slot, customer_select_tuple);
    TERRIER_ASSERT(select_result, "Customer table doesn't change (no new entries). All lookups should succeed.");

    const auto c_id =
        !args.use_c_last
            ? args.c_id
            : *reinterpret_cast<int32_t *>(customer_select_tuple->AccessWithNullCheck(c_id_select_pr_offset));
    const auto c_balance =
        *reinterpret_cast<double *>(customer_select_tuple->AccessWithNullCheck(c_balance_select_pr_offset));
    const auto c_ytd_payment =
        *reinterpret_cast<double *>(customer_select_tuple->AccessWithNullCheck(c_ytd_payment_select_pr_offset));
    const auto c_payment_cnt =
        *reinterpret_cast<int16_t *>(customer_select_tuple->AccessWithNullCheck(c_payment_cnt_select_pr_offset));
    const auto c_credit = *reinterpret_cast<storage::VarlenEntry *>(
        customer_select_tuple->AccessWithNullCheck(c_credit_select_pr_offset));
    const auto c_data =
        *reinterpret_cast<storage::VarlenEntry *>(customer_select_tuple->AccessWithNullCheck(c_data_select_pr_offset));
    TERRIER_ASSERT(c_id >= 1 && c_id <= 3000, "Invalid c_id read from the Customer table.");

    // Update customer
    auto *customer_update_tuple = customer_update_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
    *reinterpret_cast<double *>(customer_update_tuple->AccessForceNotNull(c_balance_update_pr_offset)) =
        c_balance - args.h_amount;
    *reinterpret_cast<double *>(customer_update_tuple->AccessForceNotNull(c_ytd_payment_update_pr_offset)) =
        c_ytd_payment + args.h_amount;
    *reinterpret_cast<int16_t *>(customer_update_tuple->AccessForceNotNull(c_payment_cnt_update_pr_offset)) =
        static_cast<int16_t>(c_payment_cnt + 1);

    result = db->customer_table_->Update(txn, customer_slot, *customer_update_tuple);
    TERRIER_ASSERT(result, "Customer update failed. This assertion assumes 1:1 mapping between warehouse and workers.");

    const auto c_credit_str = c_credit.StringView();
    TERRIER_ASSERT(c_credit_str.compare("BC") == 0 || c_credit_str.compare("GC") == 0,
                   "Invalid c_credit read from the Customer table.");
    if (c_credit_str.compare("BC") == 0) {
      auto *const c_data_update_tuple = c_data_pr_initializer.InitializeRow(worker->customer_tuple_buffer);
      const auto c_data_str = c_data.StringView();
      auto new_c_data = std::to_string(c_id);
      new_c_data.append(std::to_string(args.c_d_id));
      new_c_data.append(std::to_string(args.c_w_id));
      new_c_data.append(std::to_string(args.d_id));
      new_c_data.append(std::to_string(args.w_id));
      new_c_data.append(std::to_string(args.h_amount));
      new_c_data.append(c_data_str);
      const auto new_c_data_length = std::min(new_c_data.length(), static_cast<std::size_t>(500));
      auto *const varlen = common::AllocationUtil::AllocateAligned(new_c_data_length);
      std::memcpy(varlen, new_c_data.data(), new_c_data_length);
      const auto varlen_entry = storage::VarlenEntry::Create(varlen, static_cast<uint32_t>(new_c_data_length), true);

      *reinterpret_cast<storage::VarlenEntry *>(c_data_update_tuple->AccessForceNotNull(0)) = varlen_entry;

      result = db->customer_table_->Update(txn, customer_slot, *c_data_update_tuple);
      TERRIER_ASSERT(result,
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
    auto *const history_insert_tuple = history_insert_pr_initializer.InitializeRow(worker->history_tuple_buffer);
    *reinterpret_cast<int32_t *>(history_insert_tuple->AccessForceNotNull(h_c_id_insert_pr_offset)) = c_id;
    *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_c_d_id_insert_pr_offset)) = args.c_d_id;
    *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_c_w_id_insert_pr_offset)) = args.c_w_id;
    *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_d_id_insert_pr_offset)) = args.d_id;
    *reinterpret_cast<int8_t *>(history_insert_tuple->AccessForceNotNull(h_w_id_insert_pr_offset)) = args.w_id;
    *reinterpret_cast<uint64_t *>(history_insert_tuple->AccessForceNotNull(h_date_insert_pr_offset)) = args.h_date;
    *reinterpret_cast<double *>(history_insert_tuple->AccessForceNotNull(h_amount_insert_pr_offset)) = args.h_amount;
    *reinterpret_cast<storage::VarlenEntry *>(history_insert_tuple->AccessForceNotNull(h_data_insert_pr_offset)) =
        h_data;

    db->history_table_->Insert(txn, *history_insert_tuple);

    txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    return true;
  }
};

}  // namespace terrier::tpcc
