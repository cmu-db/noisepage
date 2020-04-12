#include "execution/sql/cte_scan_iterator.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::sql {

  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

  CteScanIterator::CteScanIterator(terrier::execution::exec::ExecutionContext *exec_ctx,
  uint32_t *schema_cols_type, uint32_t num_schema_cols) : exec_ctx_(exec_ctx),
  cte_table_oid_(static_cast<catalog::table_oid_t>(999)), table_redo_(nullptr)
  {
  // Create column metadata for every column.
  std::vector<catalog::Schema::Column> all_columns;
  for (uint32_t i = 0; i < num_schema_cols; i++) {
    catalog::Schema::Column col("col" + std::to_string(i+1),
                                static_cast<type::TypeId>(schema_cols_type[i]), false,
                                DummyCVE(), static_cast<catalog::col_oid_t>(i+1));
    all_columns.push_back(col);
    col_oids_.push_back(static_cast<catalog::col_oid_t>(i+1));
  }

  // Create the table in the catalog.
  catalog::Schema cte_table_schema(all_columns);
  cte_table_ = new storage::SqlTable(exec_ctx->GetAccessor()->GetBlockStore(), cte_table_schema);
  }

  storage::ProjectedRow * CteScanIterator::GetInsertTempTablePR() {
    // We need all the columns
    storage::ProjectedRowInitializer pri = cte_table_->InitializerForProjectedRow(col_oids_);
    auto txn = exec_ctx_->GetTxn();
    table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), cte_table_oid_, pri);
    return table_redo_->Delta();
  }

  storage::TupleSlot CteScanIterator::TableInsert() {
    exec_ctx_->RowsAffected()++;  // believe this should only happen in root plan nodes, so should reflect count of query
    return cte_table_->Insert(exec_ctx_->GetTxn(), table_redo_);
  }

  storage::SqlTable* CteScanIterator::GetTable() {
    return cte_table_;
  }

  catalog::table_oid_t CteScanIterator::GetTableOid() {
    return cte_table_oid_;
  }
}