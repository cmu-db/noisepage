#include <execution/sql/inserter.h>
#include <algorithm>

terrier::execution::sql::Inserter::Inserter(terrier::execution::exec::ExecutionContext *exec_ctx,
                                            terrier::catalog::table_oid_t table_oid)
    : table_oid_{table_oid}, exec_ctx_{exec_ctx} {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);
  auto columns = exec_ctx_->GetAccessor()->GetSchema(table_oid_).GetColumns();
  for (const auto &col : columns) {
    col_oids_.push_back(col.Oid());
  }

  // getting index pr size
  uint32_t index_pr_size = 0;
  auto index_oids = exec_ctx->GetAccessor()->GetIndexOids(table_oid_);
  for (auto index_oid : index_oids) {
    index_pr_size = std::max(index_pr_size, GetIndex(index_oid)->GetProjectedRowInitializer().ProjectedRowSize());
  }

  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size, sizeof(uint64_t), true);
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
  // cache?
  auto index = GetIndex(index_oid);
  auto index_pri = index->GetProjectedRowInitializer();
  index_pr_ = index->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

terrier::storage::TupleSlot terrier::execution::sql::Inserter::TableInsert() {
  table_tuple_slot_ = table_->Insert(exec_ctx_->GetTxn(), table_redo_);
  return table_tuple_slot_;
}

bool terrier::execution::sql::Inserter::IndexInsert(terrier::catalog::index_oid_t index_oid) {
  auto index = GetIndex(index_oid);
  return index->Insert(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot_);
}

terrier::common::ManagedPointer<terrier::storage::index::Index> terrier::execution::sql::Inserter::GetIndex(
    terrier::catalog::index_oid_t index_oid) {
  auto iter = index_cache_.find(index_oid);
  terrier::common::ManagedPointer<terrier::storage::index::Index> index = nullptr;
  if (iter == index_cache_.end()) {
    index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
    index_cache_[index_oid] = index;
  } else {
    index = iter->second;
  }
  return index;
}
