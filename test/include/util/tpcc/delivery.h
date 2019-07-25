#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
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
 * Delivery transaction according to section 2.7.4 of the specification
 */
class Delivery {
 private:
  // New Order metadata
  const catalog::indexkeycol_oid_t no_o_id_key_oid;
  const catalog::indexkeycol_oid_t no_d_id_key_oid;
  const catalog::indexkeycol_oid_t no_w_id_key_oid;
  const storage::ProjectedRowInitializer new_order_pr_initializer;
  const uint8_t no_o_id_key_pr_offset;
  const uint8_t no_d_id_key_pr_offset;
  const uint8_t no_w_id_key_pr_offset;

  // Order metadata
  const catalog::indexkeycol_oid_t o_id_key_oid;
  const catalog::indexkeycol_oid_t o_d_id_key_oid;
  const catalog::indexkeycol_oid_t o_w_id_key_oid;
  const storage::ProjectedRowInitializer order_select_pr_initializer;
  const storage::ProjectedRowInitializer order_update_pr_initializer;
  const uint8_t o_id_key_pr_offset;
  const uint8_t o_d_id_key_pr_offset;
  const uint8_t o_w_id_key_pr_offset;

  // Order Line metadata
  const catalog::col_oid_t ol_amount_oid;
  const catalog::col_oid_t ol_delivery_d_oid;
  const catalog::indexkeycol_oid_t ol_o_id_key_oid;
  const catalog::indexkeycol_oid_t ol_d_id_key_oid;
  const catalog::indexkeycol_oid_t ol_w_id_key_oid;
  const catalog::indexkeycol_oid_t ol_number_key_oid;
  const storage::ProjectedRowInitializer order_line_select_pr_initializer;
  const storage::ProjectedRowInitializer order_line_update_pr_initializer;
  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;

  // Customer metadata
  const catalog::col_oid_t c_balance_oid;
  const catalog::col_oid_t c_delivery_cnt_oid;
  const catalog::indexkeycol_oid_t c_id_key_oid;
  const catalog::indexkeycol_oid_t c_d_id_key_oid;
  const catalog::indexkeycol_oid_t c_w_id_key_oid;
  const storage::ProjectedRowInitializer customer_pr_initializer;
  const storage::ProjectionMap customer_pr_map;
  const uint8_t c_balance_pr_offset;
  const uint8_t c_delivery_cnt_pr_offset;
  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;

 public:
  explicit Delivery(const Database *const db)
      : no_o_id_key_oid(db->new_order_primary_index_schema_.GetColumn(2).Oid()),
        no_d_id_key_oid(db->new_order_primary_index_schema_.GetColumn(1).Oid()),
        no_w_id_key_oid(db->new_order_primary_index_schema_.GetColumn(0).Oid()),

        new_order_pr_initializer(
            db->new_order_table_->InitializerForProjectedRow({db->new_order_schema_.GetColumn(0).Oid()}).first),
        no_o_id_key_pr_offset(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_o_id_key_oid))),
        no_d_id_key_pr_offset(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_d_id_key_oid))),
        no_w_id_key_pr_offset(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_w_id_key_oid))),

        o_id_key_oid(db->order_primary_index_schema_.GetColumn(2).Oid()),
        o_d_id_key_oid(db->order_primary_index_schema_.GetColumn(1).Oid()),
        o_w_id_key_oid(db->order_primary_index_schema_.GetColumn(0).Oid()),

        order_select_pr_initializer(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(3).Oid()}).first),
        order_update_pr_initializer(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(5).Oid()}).first),
        o_id_key_pr_offset(static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_id_key_oid))),
        o_d_id_key_pr_offset(static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_d_id_key_oid))),
        o_w_id_key_pr_offset(static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_w_id_key_oid))),

        ol_amount_oid(db->order_line_schema_.GetColumn(8).Oid()),
        ol_delivery_d_oid(db->order_line_schema_.GetColumn(6).Oid()),
        ol_o_id_key_oid(db->order_line_primary_index_schema_.GetColumn(2).Oid()),
        ol_d_id_key_oid(db->order_line_primary_index_schema_.GetColumn(1).Oid()),
        ol_w_id_key_oid(db->order_line_primary_index_schema_.GetColumn(0).Oid()),
        ol_number_key_oid(db->order_line_primary_index_schema_.GetColumn(3).Oid()),

        order_line_select_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(8).Oid()}).first),
        order_line_update_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(6).Oid()}).first),
        ol_o_id_key_pr_offset(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_o_id_key_oid))),
        ol_d_id_key_pr_offset(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_d_id_key_oid))),
        ol_w_id_key_pr_offset(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_w_id_key_oid))),
        ol_number_key_pr_offset(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_number_key_oid))),

        c_balance_oid(db->customer_schema_.GetColumn(16).Oid()),
        c_delivery_cnt_oid(db->customer_schema_.GetColumn(19).Oid()),
        c_id_key_oid(db->customer_primary_index_schema_.GetColumn(2).Oid()),
        c_d_id_key_oid(db->customer_primary_index_schema_.GetColumn(1).Oid()),
        c_w_id_key_oid(db->customer_primary_index_schema_.GetColumn(0).Oid()),

        customer_pr_initializer(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_delivery_cnt_oid}).first),
        customer_pr_map(db->customer_table_->InitializerForProjectedRow({c_balance_oid, c_delivery_cnt_oid}).second),
        c_balance_pr_offset(static_cast<uint8_t>(customer_pr_map.at(c_balance_oid))),
        c_delivery_cnt_pr_offset(static_cast<uint8_t>(customer_pr_map.at(c_delivery_cnt_oid))),
        c_id_key_pr_offset(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_id_key_oid))),
        c_d_id_key_pr_offset(
            static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_d_id_key_oid))),
        c_w_id_key_pr_offset(
            static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_w_id_key_oid)))

  {}

  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
