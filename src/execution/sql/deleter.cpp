#pragma once
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

#include "execution/sql/deleter.h"

namespace terrier::execution::sql {

Deleter::Deleter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid)
    : table_oid_(table_oid), exec_ctx_(exec_ctx) {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);

  uint32_t index_pr_size = 0;
  auto index_oids = exec_ctx->GetAccessor()->GetIndexes(table_oid_);
  for (auto index_oid : index_oids) {
    index_pr_size = MAX(index_pr_size, GetIndex(index_oid)->GetProjectedRowInitializer().ProjectedRowSize());
  }
  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size, sizeof(uint64_t), true);
}

common::ManagedPointer<storage::index::Index> execution::sql::Deleter::GetIndex(catalog::index_oid_t index_oid) {
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

storage::ProjectedRow *Deleter::GetIndexPR(catalog::index_oid_t index_oid) {
  auto index = GetIndex(index_oid);
  index_pr_ = index->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

bool Deleter::TableDelete(storage::TupleSlot table_tuple_slot) {
  return table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
}

void Deleter::IndexDelete(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot) {
  auto index = GetIndex(index_oid);
  index->Delete(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}
}  // namespace terrier::execution::sql