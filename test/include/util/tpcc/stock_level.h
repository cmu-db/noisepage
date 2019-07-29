#pragma once

#include <unordered_map>
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
 * Stock-Level transaction according to section 2.8.2 of the specification
 */
class StockLevel {
 private:
  // District metadata
  const storage::ProjectedRowInitializer district_select_pr_initializer;
  const uint8_t d_id_key_pr_offset;
  const uint8_t d_w_id_key_pr_offset;

  // Order Line metadata
  const storage::ProjectedRowInitializer order_line_select_pr_initializer;
  const uint8_t ol_o_id_key_pr_offset;
  const uint8_t ol_d_id_key_pr_offset;
  const uint8_t ol_w_id_key_pr_offset;
  const uint8_t ol_number_key_pr_offset;

  // Stock metadata
  const storage::ProjectedRowInitializer stock_select_pr_initializer;
  const uint8_t s_w_id_key_pr_offset;
  const uint8_t s_i_id_key_pr_offset;

 public:
  explicit StockLevel(const Database *const db)
      : district_select_pr_initializer(
            db->district_table_->InitializerForProjectedRow({db->district_schema_.GetColumn(10).Oid()}).first),
        d_id_key_pr_offset(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(1).Oid()))),
        d_w_id_key_pr_offset(static_cast<uint8_t>(db->district_primary_index_->GetKeyOidToOffsetMap().at(
            db->district_primary_index_schema_.GetColumn(0).Oid()))),
        order_line_select_pr_initializer(
            db->order_line_table_->InitializerForProjectedRow({db->order_line_schema_.GetColumn(4).Oid()}).first),
        ol_o_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(2).Oid()))),
        ol_d_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(1).Oid()))),
        ol_w_id_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(0).Oid()))),
        ol_number_key_pr_offset(static_cast<uint8_t>(db->order_line_primary_index_->GetKeyOidToOffsetMap().at(
            db->order_line_primary_index_schema_.GetColumn(3).Oid()))),
        stock_select_pr_initializer(
            db->stock_table_->InitializerForProjectedRow({db->stock_schema_.GetColumn(2).Oid()}).first),
        s_w_id_key_pr_offset(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(0).Oid()))),
        s_i_id_key_pr_offset(static_cast<uint8_t>(
            db->stock_primary_index_->GetKeyOidToOffsetMap().at(db->stock_primary_index_schema_.GetColumn(1).Oid()))) {}

  bool Execute(transaction::TransactionManager *txn_manager, Database *db, Worker *worker,
               const TransactionArgs &args) const;
};

}  // namespace terrier::tpcc
