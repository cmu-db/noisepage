#pragma once

#include <vector>

#include "benchmark_util/data_table_benchmark_util.h"
#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/tpcc_defs.h"
#include "test_util/tpcc/util.h"
#include "test_util/tpcc/worker.h"
#include "transaction/transaction_manager.h"

namespace terrier::tpcc {

/**
 * Delivery transaction according to section 2.7.4 of the specification
 */
class Delivery {
 private:
  // New Order metadata
  const catalog::indexkeycol_oid_t no_o_id_key_oid_;
  const catalog::indexkeycol_oid_t no_d_id_key_oid_;
  const catalog::indexkeycol_oid_t no_w_id_key_oid_;
  const storage::ProjectedRowInitializer new_order_pr_initializer_;
  const uint8_t no_o_id_key_pr_offset_;
  const uint8_t no_d_id_key_pr_offset_;
  const uint8_t no_w_id_key_pr_offset_;

  // Order metadata
  const catalog::indexkeycol_oid_t o_id_key_oid_;
  const catalog::indexkeycol_oid_t o_d_id_key_oid_;
  const catalog::indexkeycol_oid_t o_w_id_key_oid_;
  const storage::ProjectedRowInitializer order_select_pr_initializer_;
  const storage::ProjectedRowInitializer order_update_pr_initializer_;
  const uint8_t o_id_key_pr_offset_;
  const uint8_t o_d_id_key_pr_offset_;
  const uint8_t o_w_id_key_pr_offset_;

  // Order Line metadata
  const catalog::col_oid_t ol_amount_oid_;
  const catalog::col_oid_t ol_delivery_d_oid_;
  const catalog::indexkeycol_oid_t ol_o_id_key_oid_;
  const catalog::indexkeycol_oid_t ol_d_id_key_oid_;
  const catalog::indexkeycol_oid_t ol_w_id_key_oid_;
  const catalog::indexkeycol_oid_t ol_number_key_oid_;
  const storage::ProjectedRowInitializer order_line_select_pr_initializer_;
  const storage::ProjectedRowInitializer order_line_update_pr_initializer_;
  const uint8_t ol_o_id_key_pr_offset_;
  const uint8_t ol_d_id_key_pr_offset_;
  const uint8_t ol_w_id_key_pr_offset_;
  const uint8_t ol_number_key_pr_offset_;

  // Customer metadata
  const catalog::col_oid_t c_balance_oid_;
  const catalog::col_oid_t c_delivery_cnt_oid_;
  const catalog::indexkeycol_oid_t c_id_key_oid_;
  const catalog::indexkeycol_oid_t c_d_id_key_oid_;
  const catalog::indexkeycol_oid_t c_w_id_key_oid_;
  const storage::ProjectedRowInitializer customer_pr_initializer_;
  const storage::ProjectionMap customer_pr_map_;
  const uint8_t c_balance_pr_offset_;
  const uint8_t c_delivery_cnt_pr_offset_;
  const uint8_t c_id_key_pr_offset_;
  const uint8_t c_d_id_key_pr_offset_;
  const uint8_t c_w_id_key_pr_offset_;

 public:
  explicit Delivery(const Database *const db)
      : no_o_id_key_oid_(db->new_order_primary_index_schema_.GetColumn(2).Oid()),
        no_d_id_key_oid_(db->new_order_primary_index_schema_.GetColumn(1).Oid()),
        no_w_id_key_oid_(db->new_order_primary_index_schema_.GetColumn(0).Oid()),

        new_order_pr_initializer_(
            db->new_order_table_->InitializerForProjectedRow({db->new_order_schema_.GetColumn(0).Oid()})),
        no_o_id_key_pr_offset_(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_o_id_key_oid_))),
        no_d_id_key_pr_offset_(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_d_id_key_oid_))),
        no_w_id_key_pr_offset_(
            static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(no_w_id_key_oid_))),

        o_id_key_oid_(db->order_primary_index_schema_.GetColumn(2).Oid()),
        o_d_id_key_oid_(db->order_primary_index_schema_.GetColumn(1).Oid()),
        o_w_id_key_oid_(db->order_primary_index_schema_.GetColumn(0).Oid()),

        order_select_pr_initializer_(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(3).Oid()})),
        order_update_pr_initializer_(
            db->order_table_->InitializerForProjectedRow({db->order_schema_.GetColumn(5).Oid()})),
        o_id_key_pr_offset_(static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_id_key_oid_))),
        o_d_id_key_pr_offset_(
            static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_d_id_key_oid_))),
        o_w_id_key_pr_offset_(
            static_cast<uint8_t>(db->order_primary_index_->GetKeyOidToOffsetMap().at(o_w_id_key_oid_))),

        ol_amount_oid_(db->order_line_schema_.GetColumn(8).Oid()),
        ol_delivery_d_oid_(db->order_line_schema_.GetColumn(6).Oid()),
        ol_o_id_key_oid_(db->order_line_primary_index_schema_.GetColumn(2).Oid()),
        ol_d_id_key_oid_(db->order_line_primary_index_schema_.GetColumn(1).Oid()),
        ol_w_id_key_oid_(db->order_line_primary_index_schema_.GetColumn(0).Oid()),
        ol_number_key_oid_(db->order_line_primary_index_schema_.GetColumn(3).Oid()),

        order_line_select_pr_initializer_(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(8).Oid()})),
        order_line_update_pr_initializer_(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(6).Oid()})),
        ol_o_id_key_pr_offset_(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_o_id_key_oid_))),
        ol_d_id_key_pr_offset_(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_d_id_key_oid_))),
        ol_w_id_key_pr_offset_(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_w_id_key_oid_))),
        ol_number_key_pr_offset_(
            static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(ol_number_key_oid_))),

        c_balance_oid_(db->customer_schema_.GetColumn(16).Oid()),
        c_delivery_cnt_oid_(db->customer_schema_.GetColumn(19).Oid()),
        c_id_key_oid_(db->customer_primary_index_schema_.GetColumn(2).Oid()),
        c_d_id_key_oid_(db->customer_primary_index_schema_.GetColumn(1).Oid()),
        c_w_id_key_oid_(db->customer_primary_index_schema_.GetColumn(0).Oid()),

        customer_pr_initializer_(
            db->customer_table_->InitializerForProjectedRow({c_balance_oid_, c_delivery_cnt_oid_})),
        customer_pr_map_(db->customer_table_->ProjectionMapForOids({c_balance_oid_, c_delivery_cnt_oid_})),
        c_balance_pr_offset_(static_cast<uint8_t>(customer_pr_map_.at(c_balance_oid_))),
        c_delivery_cnt_pr_offset_(static_cast<uint8_t>(customer_pr_map_.at(c_delivery_cnt_oid_))),
        c_id_key_pr_offset_(
            static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_id_key_oid_))),
        c_d_id_key_pr_offset_(
            static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_d_id_key_oid_))),
        c_w_id_key_pr_offset_(
            static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(c_w_id_key_oid_)))

  {}

  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
