#include "execution/sql/ind_cte_scan_iterator.h"

#include "catalog/catalog_accessor.h"
#include "execution/sql/cte_scan_iterator.h"
#include "parser/expression/constant_value_expression.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::sql {

IndCteScanIterator::IndCteScanIterator(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                                       uint32_t *schema_cols_ids, uint32_t *schema_cols_type, uint32_t num_schema_cols,
                                       bool is_recursive)
    : exec_ctx_{exec_ctx},
      cte_scan_1_{
          exec_ctx,
          catalog::table_oid_t(catalog::MakeTempOid<catalog::table_oid_t>(exec_ctx->GetAccessor()->GetNewTempOid())),
          schema_cols_ids, schema_cols_type, num_schema_cols},
      cte_scan_2_{
          exec_ctx,
          catalog::table_oid_t(catalog::MakeTempOid<catalog::table_oid_t>(exec_ctx->GetAccessor()->GetNewTempOid())),
          schema_cols_ids, schema_cols_type, num_schema_cols},
      cte_scan_3_{
          exec_ctx,
          catalog::table_oid_t(catalog::MakeTempOid<catalog::table_oid_t>(exec_ctx->GetAccessor()->GetNewTempOid())),
          schema_cols_ids, schema_cols_type, num_schema_cols},
      cte_scan_read_{&cte_scan_2_},
      cte_scan_write_{&cte_scan_3_},
      table_oid_{table_oid},
      txn_{exec_ctx->GetTxn()},
      written_{false},
      is_recursive_{is_recursive} {}

CteScanIterator *IndCteScanIterator::GetWriteCte() { return cte_scan_write_; }

CteScanIterator *IndCteScanIterator::GetReadCte() {
  exec_ctx_->GetAccessor()->RegisterTempTable(table_oid_, common::ManagedPointer(cte_scan_read_->GetTable()));
  return cte_scan_read_;
}

CteScanIterator *IndCteScanIterator::GetResultCTE() {
  if (is_recursive_) {
    exec_ctx_->GetAccessor()->RegisterTempTable(table_oid_, common::ManagedPointer(cte_scan_1_.GetTable()));
    return &cte_scan_1_;
  }
  exec_ctx_->GetAccessor()->RegisterTempTable(table_oid_, common::ManagedPointer(cte_scan_read_->GetTable()));
  return cte_scan_read_;
}

catalog::table_oid_t IndCteScanIterator::GetReadTableOid() { return cte_scan_read_->GetTableOid(); }

storage::ProjectedRow *IndCteScanIterator::GetInsertTempTablePR() { return cte_scan_write_->GetInsertTempTablePR(); }

storage::TupleSlot IndCteScanIterator::TableInsert() {
  written_ = true;
  return cte_scan_write_->TableInsert();
}

bool IndCteScanIterator::Accumulate() {
  // Dump contents from read table into table_1, and then swap read and write
  // dump read table into table_1
  if (is_recursive_) {
    // dump read table into table_1
    cte_scan_1_.GetTable()->CopyTable(txn_, common::ManagedPointer(cte_scan_read_->GetTable()));
  }

  if (written_) {
    // swap the table
    auto temp_table = cte_scan_write_;
    cte_scan_write_ = cte_scan_read_;
    cte_scan_read_ = temp_table;

    // clear new write table
    cte_scan_write_->GetTable()->Reset();
  }

  bool old_written = written_;
  written_ = false;
  return old_written;
}

}  // namespace noisepage::execution::sql
