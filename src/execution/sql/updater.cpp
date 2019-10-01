#pragma once
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

#include "execution/sql/updater.h"

namespace terrier::execution::sql {

Updater::Updater(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                 std::vector<catalog::col_oid_t> col_oids)
    : table_oid_(table_oid), col_oids_(col_oids), exec_ctx_(exec_ctx) {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);

  uint32_t index_pr_size = 0;
  auto index_oids = exec_ctx->GetAccessor()->GetIndexes(table_oid_);
  for (auto index_oid : index_oids) {
    index_pr_size = MAX(index_pr_size, GetIndex(index_oid)->GetProjectedRowInitializer().ProjectedRowSize());
  }
  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size, sizeof(uint64_t), true);
}

common::ManagedPointer<storage::index::Index> execution::sql::Updater::GetIndex(catalog::index_oid_t index_oid) {
  auto iter = index_cache_.find(index_oid);
  common::ManagedPointer<storage::index::Index> index = nullptr;
  if (iter == index_cache_.end()) {
    index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
    index_cache_[index_oid] = index;
  } else {
    index = iter->second;
  }
  return index;
}

storage::ProjectedRow *Updater::GetTablePR() {
  // We need all the columns
  storage::ProjectedRowInitializer pri = table_->InitializerForProjectedRow(col_oids_);
  auto txn = exec_ctx_->GetTxn();
  table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri);
  return table_redo_->Delta();
}

storage::ProjectedRow *Updater::GetIndexPR(catalog::index_oid_t index_oid) {
  auto index = GetIndex(index_oid);
  index_pr_ = index->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

bool Updater::TableUpdate(storage::TupleSlot table_tuple_slot) {
  table_redo_->SetTupleSlot(table_tuple_slot);
  return table_->Update(exec_ctx_->GetTxn(), table_redo_);
}

bool Updater::IndexInsert(catalog::index_oid_t index_oid) {
  auto index = GetIndex(index_oid);
  return index->Insert(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}

void Updater::IndexDelete(catalog::index_oid_t index_oid) {
  auto index = GetIndex(index_oid);
  index->Delete(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}
}  // namespace terrier::execution::sql