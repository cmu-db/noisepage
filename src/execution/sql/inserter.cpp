
#include <execution/sql/inserter.h>
#include <stdio.h>

#include "execution/sql/inserter.h"
terrier::execution::sql::Inserter::Inserter(terrier::execution::exec::ExecutionContext *exec_ctx,
                                            terrier::catalog::table_oid_t table_oid) : table_oid_{table_oid},
                                            exec_ctx_{exec_ctx} {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);
  auto columns = exec_ctx_->GetAccessor()->GetSchema(table_oid_).GetColumns();
  for(auto col : columns) {
    col_oids_.push_back(col.Oid());
  }
}

terrier::storage::ProjectedRow *terrier::execution::sql::Inserter::GetTablePR() {
  // We need all the columns
  terrier::storage::ProjectedRowInitializer pri = table_->InitializerForProjectedRow(col_oids_);
  auto txn = exec_ctx_->GetTxn();
  table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri);
  table_pr_ = table_redo_->Delta();

  return table_pr_;
}

terrier::storage::ProjectedRow *terrier::execution::sql::Inserter::GetIndexPR(terrier::catalog::index_oid_t index_oid) {
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  terrier::storage::ProjectedRowInitializer pri = index->GetProjectedRowInitializer();
  auto txn = exec_ctx_->GetTxn();
  index_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri);
  index_pr_ = index_redo_->Delta();
  return index_pr_;
}
terrier::storage::TupleSlot terrier::execution::sql::Inserter::TableInsert() {
  table_tuple_slot_ = table_->Insert(exec_ctx_->GetTxn(), table_redo_);
  size_t count2 = 0;
  for (auto iter = table_->begin(); iter != table_->end(); iter++) {
    count2++;
  }
  return table_tuple_slot_;
}
bool terrier::execution::sql::Inserter::IndexInsert(terrier::catalog::index_oid_t index_oid) {
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  return index->Insert(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot_);
}
