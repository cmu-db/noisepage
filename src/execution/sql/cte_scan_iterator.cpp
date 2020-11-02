#include "execution/sql/cte_scan_iterator.h"

#include "catalog/catalog_accessor.h"
#include "catalog/schema.h"
#include "parser/expression/constant_value_expression.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::sql {

CteScanIterator::CteScanIterator(noisepage::execution::exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                                 uint32_t *schema_cols_ids, uint32_t *schema_cols_type, uint32_t num_schema_cols)
    : exec_ctx_(exec_ctx), cte_table_oid_(table_oid), table_redo_(nullptr) {
  // Create column metadata for every column.
  std::vector<catalog::Schema::Column> all_columns;
  for (uint32_t i = 0; i < num_schema_cols; i++) {
    catalog::Schema::Column col("col" + std::to_string(i), static_cast<type::TypeId>(schema_cols_type[i]), false,
                                parser::ConstantValueExpression(static_cast<type::TypeId>(schema_cols_type[i])),
                                catalog::col_oid_t(schema_cols_ids[i]));
    all_columns.push_back(col);
    col_oids_.emplace_back(catalog::col_oid_t(schema_cols_ids[i]));
  }

  // Create the table in the catalog.
  catalog::Schema cte_table_schema(all_columns);
  cte_table_ = new storage::SqlTable(exec_ctx->GetAccessor()->GetBlockStore(), cte_table_schema);
  exec_ctx->GetAccessor()->RegisterTempTable(table_oid, common::ManagedPointer(cte_table_));

  auto cte_table_local = cte_table_;

  // We are deferring it in both commit and abort because we need to delete the temp table regardless of transaction
  // outcome. We use deferred actions to guarantee memory safety with the garbage collector.
  // TODO(Rohan): explore API change to deferred actions for unconditional actions to avoid commit and abort actions
  exec_ctx_->GetTxn()->RegisterCommitAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      deferred_action_manager->RegisterDeferredAction([=]() {
        // Defer an action upon commit to delete the table. Delete table will need a double deferral because there could
        // be transactions not yet unlinked by the GC that depend on the table
        delete cte_table_local;
      });
    });
  });

  exec_ctx_->GetTxn()->RegisterAbortAction([=](transaction::DeferredActionManager *deferred_action_manager) {
    deferred_action_manager->RegisterDeferredAction([=]() {
      deferred_action_manager->RegisterDeferredAction([=]() {
        // Defer an action upon abort to delete the table. Delete table will need a double deferral because there could
        // be transactions not yet unlinked by the GC that depend on the table
        delete cte_table_local;
      });
    });
  });
}

storage::ProjectedRow *CteScanIterator::GetInsertTempTablePR() {
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

storage::SqlTable *CteScanIterator::GetTable() { return cte_table_; }

catalog::table_oid_t CteScanIterator::GetTableOid() { return cte_table_oid_; }
}  // namespace noisepage::execution::sql
