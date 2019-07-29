#pragma once

#include <string>
#include <utility>
#include <vector>
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
 * New-Order transaction according to section 2.4.2 of the specification
 */
class NewOrder {
 private:
  // Struct for deferred index inserts after New Order is guaranteed to commit
  struct OrderLineIndexInserts {
    const int32_t o_id;
    const int8_t d_id;
    const int8_t w_id;
    const int8_t ol_number;
    storage::TupleSlot slot;
  };

  // Warehouse metadata
  const storage::ProjectedRowInitializer warehouse_select_pr_initializer;

  // District metadata
  const catalog::col_oid_t d_tax_oid;
  const catalog::col_oid_t d_next_o_id_oid;
  const storage::ProjectedRowInitializer district_select_pr_initializer;
  const storage::ProjectionMap district_select_pr_map;
  const uint8_t d_id_key_pr_offset;
  const uint8_t d_w_id_key_pr_offset;
  const uint8_t d_tax_select_pr_offset;
  const uint8_t d_next_o_id_select_pr_offset;
  const storage::ProjectedRowInitializer district_update_pr_initializer;

  // Customer metadata
  const catalog::col_oid_t c_discount_oid;
  const catalog::col_oid_t c_last_oid;
  const catalog::col_oid_t c_credit_oid;
  const storage::ProjectedRowInitializer customer_select_pr_initializer;
  const storage::ProjectionMap customer_select_pr_map;
  const uint8_t c_discount_select_pr_offset;
  const uint8_t c_id_key_pr_offset;
  const uint8_t c_d_id_key_pr_offset;
  const uint8_t c_w_id_key_pr_offset;

  // New Order metadata
  const storage::ProjectedRowInitializer new_order_insert_pr_initializer;
  const storage::ProjectionMap new_order_insert_pr_map;
  const uint8_t no_o_id_insert_pr_offset;
  const uint8_t no_d_id_insert_pr_offset;
  const uint8_t no_w_id_insert_pr_offset;
  const uint8_t no_o_id_key_pr_offset;
  const uint8_t no_d_id_key_pr_offset;
  const uint8_t no_w_id_key_pr_offset;

  // Order metadata
  const storage::ProjectedRowInitializer order_insert_pr_initializer;
  const storage::ProjectionMap order_insert_pr_map;
  const uint8_t o_id_insert_pr_offset;
  const uint8_t o_d_id_insert_pr_offset;
  const uint8_t o_w_id_insert_pr_offset;
  const uint8_t o_c_id_insert_pr_offset;
  const uint8_t o_entry_d_insert_pr_offset;
  const uint8_t o_carrier_id_insert_pr_offset;
  const uint8_t o_ol_cnt_insert_pr_offset;
  const uint8_t o_all_local_insert_pr_offset;
  const uint8_t o_id_key_pr_offset;
  const uint8_t o_d_id_key_pr_offset;
  const uint8_t o_w_id_key_pr_offset;
  const uint8_t o_id_secondary_key_pr_offset;
  const uint8_t o_d_id_secondary_key_pr_offset;
  const uint8_t o_w_id_secondary_key_pr_offset;
  const uint8_t o_c_id_secondary_key_pr_offset;

  // Item metadata
  const catalog::col_oid_t i_price_oid;
  const catalog::col_oid_t i_name_oid;
  const catalog::col_oid_t i_data_oid;
  const storage::ProjectedRowInitializer item_select_pr_initializer;
  const storage::ProjectionMap item_select_pr_map;
  const uint8_t i_price_select_pr_offset;
  const uint8_t i_data_select_pr_offset;

  // Stock metadata
  struct StockSelectPROffsets {
    const uint8_t s_quantity_select_pr_offset;
    const uint8_t s_dist_xx_select_pr_offset;
    const uint8_t s_ytd_select_pr_offset;
    const uint8_t s_order_cnt_select_pr_offset;
    const uint8_t s_remote_cnt_select_pr_offset;
    const uint8_t s_data_select_pr_offset;
  };
  const catalog::col_oid_t s_quantity_oid;
  const catalog::col_oid_t s_ytd_oid;
  const catalog::col_oid_t s_order_cnt_oid;
  const catalog::col_oid_t s_remote_cnt_oid;
  const catalog::col_oid_t s_data_oid;
  const storage::ProjectedRowInitializer stock_update_pr_initializer;
  const storage::ProjectionMap stock_update_pr_map;
  const uint8_t s_quantity_update_pr_offset;
  const uint8_t s_ytd_update_pr_offset;
  const uint8_t s_order_cnt_update_pr_offset;
  const uint8_t s_remote_cnt_update_pr_offset;
  const uint8_t s_i_id_key_pr_offset;
  const uint8_t s_w_id_key_pr_offset;
  std::vector<StockSelectPROffsets> stock_select_pr_offsets;
  std::vector<std::pair<storage::ProjectedRowInitializer, storage::ProjectionMap>> stock_select_initializers;

  // Order Line metadata
  const storage::ProjectedRowInitializer order_line_insert_pr_initializer;
  const storage::ProjectionMap order_line_insert_pr_map;
  const uint8_t ol_o_id_insert_pr_offset;
  const uint8_t ol_d_id_insert_pr_offset;
  const uint8_t ol_w_id_insert_pr_offset;
  const uint8_t ol_number_insert_pr_offset;
  const uint8_t ol_i_id_insert_pr_offset;
  const uint8_t ol_supply_w_id_insert_pr_offset;
  const uint8_t ol_delivery_d_insert_pr_offset;
  const uint8_t ol_quantity_insert_pr_offset;
  const uint8_t ol_amount_insert_pr_offset;
  const uint8_t ol_dist_info_insert_pr_offset;
  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;

 public:
  explicit NewOrder(const Database *const db)

      :  // Warehouse metadata
        warehouse_select_pr_initializer(
            db->warehouse_table_->InitializerForProjectedRow({db->warehouse_schema_.GetColumn(7).Oid()}).first),

        // District metadata
        d_tax_oid(db->district_schema_.GetColumn(8).Oid()),
        d_next_o_id_oid(db->district_schema_.GetColumn(10).Oid()),
        district_select_pr_initializer(
            db->district_table_->InitializerForProjectedRow({d_tax_oid, d_next_o_id_oid}).first),
        district_select_pr_map(db->district_table_->InitializerForProjectedRow({d_tax_oid, d_next_o_id_oid}).second),
        d_id_key_pr_offset(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(1).Oid()))),
        d_w_id_key_pr_offset(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(0).Oid()))),
        d_tax_select_pr_offset(static_cast<uint8_t>(district_select_pr_map.at(d_tax_oid))),
        d_next_o_id_select_pr_offset(static_cast<uint8_t>(district_select_pr_map.at(d_next_o_id_oid))),
        district_update_pr_initializer(db->district_table_->InitializerForProjectedRow({d_next_o_id_oid}).first),

        // Customer metadata
        c_discount_oid(db->customer_schema_.GetColumn(15).Oid()),
        c_last_oid(db->customer_schema_.GetColumn(5).Oid()),
        c_credit_oid(db->customer_schema_.GetColumn(13).Oid()),
        customer_select_pr_initializer(
            db->customer_table_->InitializerForProjectedRow({c_discount_oid, c_last_oid, c_credit_oid}).first),
        customer_select_pr_map(
            db->customer_table_->InitializerForProjectedRow({c_discount_oid, c_last_oid, c_credit_oid}).second),
        c_discount_select_pr_offset(static_cast<uint8_t>(customer_select_pr_map.at(c_discount_oid))),
        c_id_key_pr_offset(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(2).Oid()))),
        c_d_id_key_pr_offset(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(1).Oid()))),
        c_w_id_key_pr_offset(static_cast<uint8_t>(db->customer_primary_index_->GetKeyOidToOffsetMap().at(
            db->customer_primary_index_schema_.GetColumn(0).Oid()))),

        // New Order metadata
        new_order_insert_pr_initializer(
            db->new_order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->new_order_schema_)).first),
        new_order_insert_pr_map(
            db->new_order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->new_order_schema_)).second),
        no_o_id_insert_pr_offset(
            static_cast<uint8_t>(new_order_insert_pr_map.at(db->new_order_schema_.GetColumn(0).Oid()))),
        no_d_id_insert_pr_offset(
            static_cast<uint8_t>(new_order_insert_pr_map.at(db->new_order_schema_.GetColumn(1).Oid()))),
        no_w_id_insert_pr_offset(
            static_cast<uint8_t>(new_order_insert_pr_map.at(db->new_order_schema_.GetColumn(2).Oid()))),
        no_o_id_key_pr_offset(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(2).Oid()))),
        no_d_id_key_pr_offset(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(1).Oid()))),
        no_w_id_key_pr_offset(static_cast<uint8_t>(db->new_order_primary_index_->GetKeyOidToOffsetMap().at(
            db->new_order_primary_index_schema_.GetColumn(0).Oid()))),

        // Order metadata
        order_insert_pr_initializer(
            db->order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_schema_)).first),
        order_insert_pr_map(
            db->order_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_schema_)).second),
        o_id_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(0).Oid()))),
        o_d_id_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(1).Oid()))),
        o_w_id_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(2).Oid()))),
        o_c_id_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(3).Oid()))),
        o_entry_d_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(4).Oid()))),
        o_carrier_id_insert_pr_offset(
            static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(5).Oid()))),
        o_ol_cnt_insert_pr_offset(static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(6).Oid()))),
        o_all_local_insert_pr_offset(
            static_cast<uint8_t>(order_insert_pr_map.at(db->order_schema_.GetColumn(7).Oid()))),
        o_id_key_pr_offset(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(2).Oid()))),
        o_d_id_key_pr_offset(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(1).Oid()))),
        o_w_id_key_pr_offset(static_cast<uint8_t>(
            db->order_primary_index_->GetKeyOidToOffsetMap().at(db->order_primary_index_schema_.GetColumn(0).Oid()))),
        o_id_secondary_key_pr_offset(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(3).Oid()))),
        o_d_id_secondary_key_pr_offset(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(1).Oid()))),
        o_w_id_secondary_key_pr_offset(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(0).Oid()))),
        o_c_id_secondary_key_pr_offset(static_cast<uint8_t>(db->order_secondary_index_->GetKeyOidToOffsetMap().at(
            db->order_secondary_index_schema_.GetColumn(2).Oid()))),

        // Item metadata
        i_price_oid(db->item_schema_.GetColumn(3).Oid()),
        i_name_oid(db->item_schema_.GetColumn(2).Oid()),
        i_data_oid(db->item_schema_.GetColumn(4).Oid()),
        item_select_pr_initializer(
            db->item_table_->InitializerForProjectedRow({i_price_oid, i_name_oid, i_data_oid}).first),
        item_select_pr_map(db->item_table_->InitializerForProjectedRow({i_price_oid, i_name_oid, i_data_oid}).second),
        i_price_select_pr_offset(static_cast<uint8_t>(item_select_pr_map.at(i_price_oid))),
        i_data_select_pr_offset(static_cast<uint8_t>(item_select_pr_map.at(i_data_oid))),

        // Stock metadata
        s_quantity_oid(db->stock_schema_.GetColumn(2).Oid()),
        s_ytd_oid(db->stock_schema_.GetColumn(13).Oid()),
        s_order_cnt_oid(db->stock_schema_.GetColumn(14).Oid()),
        s_remote_cnt_oid(db->stock_schema_.GetColumn(15).Oid()),
        s_data_oid(db->stock_schema_.GetColumn(16).Oid()),
        stock_update_pr_initializer(
            db->stock_table_->InitializerForProjectedRow({s_quantity_oid, s_ytd_oid, s_order_cnt_oid, s_remote_cnt_oid})
                .first),
        stock_update_pr_map(
            db->stock_table_->InitializerForProjectedRow({s_quantity_oid, s_ytd_oid, s_order_cnt_oid, s_remote_cnt_oid})
                .second),

        s_quantity_update_pr_offset(static_cast<uint8_t>(stock_update_pr_map.at(s_quantity_oid))),
        s_ytd_update_pr_offset(static_cast<uint8_t>(stock_update_pr_map.at(s_ytd_oid))),
        s_order_cnt_update_pr_offset(static_cast<uint8_t>(stock_update_pr_map.at(s_order_cnt_oid))),
        s_remote_cnt_update_pr_offset(static_cast<uint8_t>(stock_update_pr_map.at(s_remote_cnt_oid))),
        s_i_id_key_pr_offset(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(1).Oid()))),
        s_w_id_key_pr_offset(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(0).Oid()))),

        // Order Line metadata
        order_line_insert_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_line_schema_)).first),
        order_line_insert_pr_map(
            db->order_line_table_->InitializerForProjectedRow(Util::AllColOidsForSchema(db->order_line_schema_))
                .second),
        ol_o_id_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(0).Oid()))),
        ol_d_id_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(1).Oid()))),
        ol_w_id_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(2).Oid()))),
        ol_number_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(3).Oid()))),
        ol_i_id_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(4).Oid()))),
        ol_supply_w_id_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(5).Oid()))),
        ol_delivery_d_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(6).Oid()))),
        ol_quantity_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(7).Oid()))),
        ol_amount_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(8).Oid()))),
        ol_dist_info_insert_pr_offset(
            static_cast<uint8_t>(order_line_insert_pr_map.at(db->order_line_schema_.GetColumn(9).Oid()))),
        ol_o_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(2).Oid()))),
        ol_d_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(1).Oid()))),
        ol_w_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(0).Oid()))),
        ol_number_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(3).Oid()))) {
    // Generate metadata for all 10 districts
    stock_select_initializers.reserve(10);
    for (uint8_t d_id = 0; d_id < 10; d_id++) {
      const auto s_dist_xx_oid = db->stock_schema_.GetColumn(3 + d_id).Oid();
      stock_select_initializers.emplace_back((db->stock_table_->InitializerForProjectedRow(
          {s_quantity_oid, s_dist_xx_oid, s_ytd_oid, s_order_cnt_oid, s_remote_cnt_oid, s_data_oid})));
    }
    stock_select_pr_offsets.reserve(10);
    for (uint8_t d_id = 0; d_id < 10; d_id++) {
      const auto s_dist_xx_oid = db->stock_schema_.GetColumn(3 + d_id).Oid();
      stock_select_pr_offsets.push_back(
          {static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_quantity_oid)),
           static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_dist_xx_oid)),
           static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_ytd_oid)),
           static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_order_cnt_oid)),
           static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_remote_cnt_oid)),
           static_cast<uint8_t>(stock_select_initializers[d_id].second.at(s_data_oid))});
    }
  }

  // 2.4.2
  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
