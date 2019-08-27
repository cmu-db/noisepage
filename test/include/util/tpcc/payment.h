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
#include "util/tpcc/tpcc_defs.h"
#include "util/tpcc/util.h"
#include "util/tpcc/worker.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::tpcc {

/**
 * Delivery transaction according to section 2.5.2 of the specification
 */
class Payment {
 private:
  // Warehouse metadata
  const catalog::col_oid_t w_name_oid_;
  const catalog::col_oid_t w_street_1_oid_;
  const catalog::col_oid_t w_street_2_oid_;
  const catalog::col_oid_t w_city_oid_;
  const catalog::col_oid_t w_state_oid_;
  const catalog::col_oid_t w_zip_oid_;
  const catalog::col_oid_t w_ytd_oid_;
  const storage::ProjectedRowInitializer warehouse_select_pr_initializer_;
  const storage::ProjectionMap warehouse_select_pr_map_;
  const uint8_t w_name_select_pr_offset_;
  const uint8_t w_ytd_select_pr_offset_;
  const storage::ProjectedRowInitializer warehouse_update_pr_initializer_;

  // District metadata
  const uint8_t d_id_key_pr_offset_;
  const uint8_t d_w_id_key_pr_offset_;
  const catalog::col_oid_t d_name_oid_;
  const catalog::col_oid_t d_street_1_oid_;
  const catalog::col_oid_t d_street_2_oid_;
  const catalog::col_oid_t d_city_oid_;
  const catalog::col_oid_t d_state_oid_;
  const catalog::col_oid_t d_zip_oid_;
  const catalog::col_oid_t d_ytd_oid_;
  const storage::ProjectedRowInitializer district_select_pr_initializer_;
  const storage::ProjectionMap district_select_pr_map_;
  const uint8_t d_name_select_pr_offset_;
  const uint8_t d_ytd_select_pr_offset_;
  const storage::ProjectedRowInitializer district_update_pr_initializer_;

  // Customer metadata
  const uint8_t c_id_key_pr_offset_;
  const uint8_t c_d_id_key_pr_offset_;
  const uint8_t c_w_id_key_pr_offset_;
  const uint8_t c_last_name_key_pr_offset_;
  const uint8_t c_d_id_name_key_pr_offset_;
  const uint8_t c_w_id_name_key_pr_offset_;
  const storage::ProjectedRowInitializer c_first_pr_initializer_;
  const storage::ProjectedRowInitializer customer_select_pr_initializer_;
  const storage::ProjectionMap customer_select_pr_map_;
  const catalog::col_oid_t c_id_oid_;
  const catalog::col_oid_t c_credit_oid_;
  const catalog::col_oid_t c_balance_oid_;
  const catalog::col_oid_t c_ytd_payment_oid_;
  const catalog::col_oid_t c_payment_cnt_oid_;
  const catalog::col_oid_t c_data_oid_;
  const uint8_t c_id_select_pr_offset_;
  const uint8_t c_credit_select_pr_offset_;
  const uint8_t c_balance_select_pr_offset_;
  const uint8_t c_ytd_payment_select_pr_offset_;
  const uint8_t c_payment_cnt_select_pr_offset_;
  const uint8_t c_data_select_pr_offset_;
  const storage::ProjectedRowInitializer customer_update_pr_initializer_;
  const storage::ProjectionMap customer_update_pr_map_;
  const uint8_t c_balance_update_pr_offset_;
  const uint8_t c_ytd_payment_update_pr_offset_;
  const uint8_t c_payment_cnt_update_pr_offset_;
  const storage::ProjectedRowInitializer c_data_pr_initializer_;

  // History metadata
  const storage::ProjectedRowInitializer history_insert_pr_initializer_;
  const storage::ProjectionMap history_insert_pr_map_;
  const uint8_t h_c_id_insert_pr_offset_;
  const uint8_t h_c_d_id_insert_pr_offset_;
  const uint8_t h_c_w_id_insert_pr_offset_;
  const uint8_t h_d_id_insert_pr_offset_;
  const uint8_t h_w_id_insert_pr_offset_;
  const uint8_t h_date_insert_pr_offset_;
  const uint8_t h_amount_insert_pr_offset_;
  const uint8_t h_data_insert_pr_offset_;

 public:
  explicit Payment(const Database *const db)

      :  // Warehouse metadata
        w_name_oid_(db->warehouse_schema_.GetColumn(1).Oid()),
        w_street_1_oid_(db->warehouse_schema_.GetColumn(2).Oid()),
        w_street_2_oid_(db->warehouse_schema_.GetColumn(3).Oid()),
        w_city_oid_(db->warehouse_schema_.GetColumn(4).Oid()),
        w_state_oid_(db->warehouse_schema_.GetColumn(5).Oid()),
        w_zip_oid_(db->warehouse_schema_.GetColumn(6).Oid()),
        w_ytd_oid_(db->warehouse_schema_.GetColumn(8).Oid()),
        warehouse_select_pr_initializer_(db->warehouse_table_->InitializerForProjectedRow(
            {w_name_oid, w_street_1_oid, w_street_2_oid, w_city_oid, w_state_oid, w_zip_oid, w_ytd_oid})),
        warehouse_select_pr_map_(db->warehouse_table_->ProjectionMapForOids(
            {w_name_oid, w_street_1_oid, w_street_2_oid, w_city_oid, w_state_oid, w_zip_oid, w_ytd_oid})),
        w_name_select_pr_offset_(static_cast<uint8_t>(warehouse_select_pr_map.at(w_name_oid))),
        w_ytd_select_pr_offset_(static_cast<uint8_t>(warehouse_select_pr_map.at(w_ytd_oid))),
        warehouse_update_pr_initializer_(db->warehouse_table_->InitializerForProjectedRow({w_ytd_oid})),

        // District metadata
        d_id_key_pr_offset_(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(1).Oid()))),
        d_w_id_key_pr_offset_(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(0).Oid()))),

        d_name_oid_(db->district_schema_.GetColumn(2).Oid()),
        d_street_1_oid_(db->district_schema_.GetColumn(3).Oid()),
        d_street_2_oid_(db->district_schema_.GetColumn(4).Oid()),
        d_city_oid_(db->district_schema_.GetColumn(5).Oid()),
        d_state_oid_(db->district_schema_.GetColumn(6).Oid()),
        d_zip_oid_(db->district_schema_.GetColumn(7).Oid()),
        d_ytd_oid_(db->district_schema_.GetColumn(9).Oid()),
        district_select_pr_initializer_(db->district_table_->InitializerForProjectedRow(
            {d_name_oid, d_street_1_oid, d_street_2_oid, d_city_oid, d_state_oid, d_zip_oid, d_ytd_oid})),
        district_select_pr_map_(db->district_table_->ProjectionMapForOids(
            {d_name_oid, d_street_1_oid, d_street_2_oid, d_city_oid, d_state_oid, d_zip_oid, d_ytd_oid})),
        d_name_select_pr_offset_(static_cast<uint8_t>(district_select_pr_map.at(d_name_oid))),
        d_ytd_select_pr_offset_(static_cast<uint8_t>(district_select_pr_map.at(d_ytd_oid))),
        district_update_pr_initializer_(db->district_table_->InitializerForProjectedRow({d_ytd_oid})),

        // Customer metadata
        c_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(2).Oid()))),
        c_d_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(1).Oid()))),
        c_w_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(0).Oid()))),
        c_last_name_key_pr_offset_(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.GetColumn(2).Oid()))),
        c_d_id_name_key_pr_offset_(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.GetColumn(1).Oid()))),
        c_w_id_name_key_pr_offset_(static_cast<uint8_t>(db->customer_secondary_index_->GetKeyOidToOffsetMap().at(
            db->customer_secondary_index_schema_.GetColumn(0).Oid()))),
        c_first_pr_initializer_(
            db->customer_table_->InitializerForProjectedRow({db->customer_schema_.GetColumn(3).Oid()})),
        customer_select_pr_initializer_(
            db->customer_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->customer_schema_))),
        customer_select_pr_map_(
            db->customer_table_->ProjectionMapForOids(Util::AllColOidsForSchema(db->customer_schema_))),

        c_id_oid_(db->customer_schema_.GetColumn(0).Oid()),
        c_credit_oid_(db->customer_schema_.GetColumn(13).Oid()),
        c_balance_oid_(db->customer_schema_.GetColumn(16).Oid()),
        c_ytd_payment_oid_(db->customer_schema_.GetColumn(17).Oid()),
        c_payment_cnt_oid_(db->customer_schema_.GetColumn(18).Oid()),
        c_data_oid_(db->customer_schema_.GetColumn(20).Oid()),

        c_id_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_id_oid))),
        c_credit_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_credit_oid))),
        c_balance_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_balance_oid))),
        c_ytd_payment_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_ytd_payment_oid))),
        c_payment_cnt_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_payment_cnt_oid))),
        c_data_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map.at(c_data_oid))),
        customer_update_pr_initializer_(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_ytd_payment_oid, c_payment_cnt_oid})),
        customer_update_pr_map_(
            db->customer_table_->ProjectionMapForOids({c_balance_oid, c_ytd_payment_oid, c_payment_cnt_oid})),
        c_balance_update_pr_offset_(static_cast<uint8_t>(customer_update_pr_map.at(c_balance_oid))),
        c_ytd_payment_update_pr_offset_(static_cast<uint8_t>(customer_update_pr_map.at(c_ytd_payment_oid))),
        c_payment_cnt_update_pr_offset_(static_cast<uint8_t>(customer_update_pr_map.at(c_payment_cnt_oid))),
        c_data_pr_initializer_(db->customer_table_->InitializerForProjectedRow({c_data_oid})),

        history_insert_pr_initializer_(
            db->history_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->history_schema_))),
        history_insert_pr_map_(db->history_table_->ProjectionMapForOids(Util::AllColOidsForSchema(db->history_schema_))),

        h_c_id_insert_pr_offset_(static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(0).Oid()))),
        h_c_d_id_insert_pr_offset_(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(1).Oid()))),
        h_c_w_id_insert_pr_offset_(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(2).Oid()))),
        h_d_id_insert_pr_offset_(static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(3).Oid()))),
        h_w_id_insert_pr_offset_(static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(4).Oid()))),
        h_date_insert_pr_offset_(static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(5).Oid()))),
        h_amount_insert_pr_offset_(
            static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(6).Oid()))),
        h_data_insert_pr_offset_(static_cast<uint8_t>(history_insert_pr_map.at(db->history_schema_.GetColumn(7).Oid())))

  {}

  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
