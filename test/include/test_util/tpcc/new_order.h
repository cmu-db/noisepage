#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/tpcc_defs.h"
#include "test_util/tpcc/util.h"
#include "test_util/tpcc/worker.h"
#include "transaction/transaction_manager.h"

namespace terrier::tpcc {

/**
 * New-Order transaction according to section 2.4.2 of the specification
 */
class NewOrder {
 private:
  // Struct for deferred index inserts after New Order is guaranteed to commit
  struct OrderLineIndexInserts {
    const int32_t o_id_;
    const int8_t d_id_;
    const int8_t w_id_;
    const int8_t ol_number_;
    storage::TupleSlot slot_;
  };

  // Warehouse metadata
  const storage::ProjectedRowInitializer warehouse_select_pr_initializer_;

  // District metadata
  const catalog::col_oid_t d_tax_oid_;
  const catalog::col_oid_t d_next_o_id_oid_;
  const storage::ProjectedRowInitializer district_select_pr_initializer_;
  const storage::ProjectionMap district_select_pr_map_;
  const uint8_t d_id_key_pr_offset_;
  const uint8_t d_w_id_key_pr_offset_;
  const uint8_t d_tax_select_pr_offset_;
  const uint8_t d_next_o_id_select_pr_offset_;
  const storage::ProjectedRowInitializer district_update_pr_initializer_;

  // Customer metadata
  const catalog::col_oid_t c_discount_oid_;
  const catalog::col_oid_t c_last_oid_;
  const catalog::col_oid_t c_credit_oid_;
  const storage::ProjectedRowInitializer customer_select_pr_initializer_;
  const storage::ProjectionMap customer_select_pr_map_;
  const uint8_t c_discount_select_pr_offset_;
  const uint8_t c_id_key_pr_offset_;
  const uint8_t c_d_id_key_pr_offset_;
  const uint8_t c_w_id_key_pr_offset_;

  // New Order metadata
  const storage::ProjectedRowInitializer new_order_insert_pr_initializer_;
  const storage::ProjectionMap new_order_insert_pr_map_;
  const uint8_t no_o_id_insert_pr_offset_;
  const uint8_t no_d_id_insert_pr_offset_;
  const uint8_t no_w_id_insert_pr_offset_;
  const uint8_t no_o_id_key_pr_offset_;
  const uint8_t no_d_id_key_pr_offset_;
  const uint8_t no_w_id_key_pr_offset_;

  // Order metadata
  const storage::ProjectedRowInitializer order_insert_pr_initializer_;
  const storage::ProjectionMap order_insert_pr_map_;
  const uint8_t o_id_insert_pr_offset_;
  const uint8_t o_d_id_insert_pr_offset_;
  const uint8_t o_w_id_insert_pr_offset_;
  const uint8_t o_c_id_insert_pr_offset_;
  const uint8_t o_entry_d_insert_pr_offset_;
  const uint8_t o_carrier_id_insert_pr_offset_;
  const uint8_t o_ol_cnt_insert_pr_offset_;
  const uint8_t o_all_local_insert_pr_offset_;
  const uint8_t o_id_key_pr_offset_;
  const uint8_t o_d_id_key_pr_offset_;
  const uint8_t o_w_id_key_pr_offset_;
  const uint8_t o_id_secondary_key_pr_offset_;
  const uint8_t o_d_id_secondary_key_pr_offset_;
  const uint8_t o_w_id_secondary_key_pr_offset_;
  const uint8_t o_c_id_secondary_key_pr_offset_;

  // Item metadata
  const catalog::col_oid_t i_price_oid_;
  const catalog::col_oid_t i_name_oid_;
  const catalog::col_oid_t i_data_oid_;
  const storage::ProjectedRowInitializer item_select_pr_initializer_;
  const storage::ProjectionMap item_select_pr_map_;
  const uint8_t i_price_select_pr_offset_;
  const uint8_t i_data_select_pr_offset_;

  // Stock metadata
  struct StockSelectPROffsets {
    const uint8_t s_quantity_select_pr_offset_;
    const uint8_t s_dist_xx_select_pr_offset_;
    const uint8_t s_ytd_select_pr_offset_;
    const uint8_t s_order_cnt_select_pr_offset_;
    const uint8_t s_remote_cnt_select_pr_offset_;
    const uint8_t s_data_select_pr_offset_;
  };
  const catalog::col_oid_t s_quantity_oid_;
  const catalog::col_oid_t s_ytd_oid_;
  const catalog::col_oid_t s_order_cnt_oid_;
  const catalog::col_oid_t s_remote_cnt_oid_;
  const catalog::col_oid_t s_data_oid_;
  const storage::ProjectedRowInitializer stock_update_pr_initializer_;
  const storage::ProjectionMap stock_update_pr_map_;
  const uint8_t s_quantity_update_pr_offset_;
  const uint8_t s_ytd_update_pr_offset_;
  const uint8_t s_order_cnt_update_pr_offset_;
  const uint8_t s_remote_cnt_update_pr_offset_;
  const uint8_t s_i_id_key_pr_offset_;
  const uint8_t s_w_id_key_pr_offset_;
  std::vector<StockSelectPROffsets> stock_select_pr_offsets_;
  std::vector<std::pair<storage::ProjectedRowInitializer, storage::ProjectionMap>> stock_select_initializers_;

  // Order Line metadata
  const storage::ProjectedRowInitializer order_line_insert_pr_initializer_;
  const storage::ProjectionMap order_line_insert_pr_map_;
  const uint8_t ol_o_id_insert_pr_offset_;
  const uint8_t ol_d_id_insert_pr_offset_;
  const uint8_t ol_w_id_insert_pr_offset_;
  const uint8_t ol_number_insert_pr_offset_;
  const uint8_t ol_i_id_insert_pr_offset_;
  const uint8_t ol_supply_w_id_insert_pr_offset_;
  const uint8_t ol_delivery_d_insert_pr_offset_;
  const uint8_t ol_quantity_insert_pr_offset_;
  const uint8_t ol_amount_insert_pr_offset_;
  const uint8_t ol_dist_info_insert_pr_offset_;
  const uint8_t ol_o_id_key_pr_offset_;
  const uint8_t ol_d_id_key_pr_offset_;
  const uint8_t ol_w_id_key_pr_offset_;
  const uint8_t ol_number_key_pr_offset_;

 public:
  explicit NewOrder(const Database *const db)

      :  // Warehouse metadata
        warehouse_select_pr_initializer_(
            db->warehouse_table_->InitializerForProjectedRow({db->warehouse_schema_.GetColumn(7).Oid()})),

        // District metadata
        d_tax_oid_(db->district_schema_.GetColumn(8).Oid()),
        d_next_o_id_oid_(db->district_schema_.GetColumn(10).Oid()),
        district_select_pr_initializer_(
            db->district_table_->InitializerForProjectedRow({d_tax_oid_, d_next_o_id_oid_})),
        district_select_pr_map_(db->district_table_->ProjectionMapForOids({d_tax_oid_, d_next_o_id_oid_})),
        d_id_key_pr_offset_(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(1).Oid()))),
        d_w_id_key_pr_offset_(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(0).Oid()))),
        d_tax_select_pr_offset_(static_cast<uint8_t>(district_select_pr_map_.at(d_tax_oid_))),
        d_next_o_id_select_pr_offset_(static_cast<uint8_t>(district_select_pr_map_.at(d_next_o_id_oid_))),
        district_update_pr_initializer_(db->district_table_->InitializerForProjectedRow({d_next_o_id_oid_})),

        // Customer metadata
        c_discount_oid_(db->customer_schema_.GetColumn(15).Oid()),
        c_last_oid_(db->customer_schema_.GetColumn(5).Oid()),
        c_credit_oid_(db->customer_schema_.GetColumn(13).Oid()),
        customer_select_pr_initializer_(
            db->customer_table_->InitializerForProjectedRow({c_discount_oid_, c_last_oid_, c_credit_oid_})),
        customer_select_pr_map_(
            db->customer_table_->ProjectionMapForOids({c_discount_oid_, c_last_oid_, c_credit_oid_})),
        c_discount_select_pr_offset_(static_cast<uint8_t>(customer_select_pr_map_.at(c_discount_oid_))),
        c_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(2).Oid()))),
        c_d_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(1).Oid()))),
        c_w_id_key_pr_offset_(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(0).Oid()))),

        // New Order metadata
        new_order_insert_pr_initializer_(
            db->new_order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->new_order_schema_))),
        new_order_insert_pr_map_(
            db->new_order_table_->ProjectionMapForOids(Util::AllColOidsForSchema(db->new_order_schema_))),
        no_o_id_insert_pr_offset_(
            static_cast<uint8_t>(new_order_insert_pr_map_.at(db->new_order_schema_.GetColumn(0).Oid()))),
        no_d_id_insert_pr_offset_(
            static_cast<uint8_t>(new_order_insert_pr_map_.at(db->new_order_schema_.GetColumn(1).Oid()))),
        no_w_id_insert_pr_offset_(
            static_cast<uint8_t>(new_order_insert_pr_map_.at(db->new_order_schema_.GetColumn(2).Oid()))),
        no_o_id_key_pr_offset_(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(2).Oid()))),
        no_d_id_key_pr_offset_(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(1).Oid()))),
        no_w_id_key_pr_offset_(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(0).Oid()))),

        // Order metadata
        order_insert_pr_initializer_(
            db->order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_schema_))),
        order_insert_pr_map_(db->order_table_->ProjectionMapForOids(Util::AllColOidsForSchema(db->order_schema_))),
        o_id_insert_pr_offset_(static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(0).Oid()))),
        o_d_id_insert_pr_offset_(static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(1).Oid()))),
        o_w_id_insert_pr_offset_(static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(2).Oid()))),
        o_c_id_insert_pr_offset_(static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(3).Oid()))),
        o_entry_d_insert_pr_offset_(
            static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(4).Oid()))),
        o_carrier_id_insert_pr_offset_(
            static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(5).Oid()))),
        o_ol_cnt_insert_pr_offset_(static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(6).Oid()))),
        o_all_local_insert_pr_offset_(
            static_cast<uint8_t>(order_insert_pr_map_.at(db->order_schema_.GetColumn(7).Oid()))),
        o_id_key_pr_offset_(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(2).Oid()))),
        o_d_id_key_pr_offset_(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(1).Oid()))),
        o_w_id_key_pr_offset_(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(0).Oid()))),
        o_id_secondary_key_pr_offset_(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(3).Oid()))),
        o_d_id_secondary_key_pr_offset_(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(1).Oid()))),
        o_w_id_secondary_key_pr_offset_(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(0).Oid()))),
        o_c_id_secondary_key_pr_offset_(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(2).Oid()))),

        // Item metadata
        i_price_oid_(db->item_schema_.GetColumn(3).Oid()),
        i_name_oid_(db->item_schema_.GetColumn(2).Oid()),
        i_data_oid_(db->item_schema_.GetColumn(4).Oid()),
        item_select_pr_initializer_(
            db->item_table_->InitializerForProjectedRow({i_price_oid_, i_name_oid_, i_data_oid_})),
        item_select_pr_map_(db->item_table_->ProjectionMapForOids({i_price_oid_, i_name_oid_, i_data_oid_})),
        i_price_select_pr_offset_(static_cast<uint8_t>(item_select_pr_map_.at(i_price_oid_))),
        i_data_select_pr_offset_(static_cast<uint8_t>(item_select_pr_map_.at(i_data_oid_))),

        // Stock metadata
        s_quantity_oid_(db->stock_schema_.GetColumn(2).Oid()),
        s_ytd_oid_(db->stock_schema_.GetColumn(13).Oid()),
        s_order_cnt_oid_(db->stock_schema_.GetColumn(14).Oid()),
        s_remote_cnt_oid_(db->stock_schema_.GetColumn(15).Oid()),
        s_data_oid_(db->stock_schema_.GetColumn(16).Oid()),
        stock_update_pr_initializer_(db->stock_table_->InitializerForProjectedRow(
            {s_quantity_oid_, s_ytd_oid_, s_order_cnt_oid_, s_remote_cnt_oid_})),
        stock_update_pr_map_(
            db->stock_table_->ProjectionMapForOids({s_quantity_oid_, s_ytd_oid_, s_order_cnt_oid_, s_remote_cnt_oid_})),

        s_quantity_update_pr_offset_(static_cast<uint8_t>(stock_update_pr_map_.at(s_quantity_oid_))),
        s_ytd_update_pr_offset_(static_cast<uint8_t>(stock_update_pr_map_.at(s_ytd_oid_))),
        s_order_cnt_update_pr_offset_(static_cast<uint8_t>(stock_update_pr_map_.at(s_order_cnt_oid_))),
        s_remote_cnt_update_pr_offset_(static_cast<uint8_t>(stock_update_pr_map_.at(s_remote_cnt_oid_))),
        s_i_id_key_pr_offset_(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(1).Oid()))),
        s_w_id_key_pr_offset_(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(0).Oid()))),

        // Order Line metadata
        order_line_insert_pr_initializer_(
            db->order_line_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_line_schema_))),
        order_line_insert_pr_map_(
            db->order_line_table_->ProjectionMapForOids(Util::AllColOidsForSchema(db->order_line_schema_))),
        ol_o_id_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(0).Oid()))),
        ol_d_id_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(1).Oid()))),
        ol_w_id_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(2).Oid()))),
        ol_number_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(3).Oid()))),
        ol_i_id_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(4).Oid()))),
        ol_supply_w_id_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(5).Oid()))),
        ol_delivery_d_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(6).Oid()))),
        ol_quantity_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(7).Oid()))),
        ol_amount_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(8).Oid()))),
        ol_dist_info_insert_pr_offset_(
            static_cast<uint8_t>(order_line_insert_pr_map_.at(db->order_line_schema_.GetColumn(9).Oid()))),
        ol_o_id_key_pr_offset_(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(2).Oid()))),
        ol_d_id_key_pr_offset_(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(1).Oid()))),
        ol_w_id_key_pr_offset_(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(0).Oid()))),
        ol_number_key_pr_offset_(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(3).Oid()))) {
    // Generate metadata for all 10 districts
    stock_select_initializers_.reserve(10);
    for (uint8_t d_id = 0; d_id < 10; d_id++) {
      const auto s_dist_xx_oid = db->stock_schema_.GetColumn(3 + d_id).Oid();
      stock_select_initializers_.emplace_back(std::pair(
          (db->stock_table_->InitializerForProjectedRow(
              {s_quantity_oid_, s_dist_xx_oid, s_ytd_oid_, s_order_cnt_oid_, s_remote_cnt_oid_, s_data_oid_})),
          db->stock_table_->ProjectionMapForOids(
              {s_quantity_oid_, s_dist_xx_oid, s_ytd_oid_, s_order_cnt_oid_, s_remote_cnt_oid_, s_data_oid_})));
    }
    stock_select_pr_offsets_.reserve(10);
    for (uint8_t d_id = 0; d_id < 10; d_id++) {
      const auto s_dist_xx_oid = db->stock_schema_.GetColumn(3 + d_id).Oid();
      stock_select_pr_offsets_.push_back(
          {static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_quantity_oid_)),
           static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_dist_xx_oid)),
           static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_ytd_oid_)),
           static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_order_cnt_oid_)),
           static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_remote_cnt_oid_)),
           static_cast<uint8_t>(stock_select_initializers_[d_id].second.at(s_data_oid_))});
    }
  }

  // 2.4.2
  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
