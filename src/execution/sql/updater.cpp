#include "execution/sql/updater.h"
#include <algorithm>
#include <vector>
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

Updater::Updater(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                 uint32_t num_oids, bool is_index_key_update)
    : table_oid_(table_oid),
      col_oids_(col_oids, col_oids + num_oids),
      exec_ctx_(exec_ctx),
      is_index_key_update_(is_index_key_update) {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);

  if (is_index_key_update) {
    index_pr_buffer_ = GetMaxSizedIndexPRBuffer();
    PopulateAllColOids(&all_col_oids_);
  }
}

void *Updater::GetMaxSizedIndexPRBuffer() {
  // Calculate the max sized index pr, and allocate that size for the index_pr_buffer
  auto index_pr_size = CalculateMaxIndexPRSize();
  return exec_ctx_->GetMemoryPool()->AllocateAligned(index_pr_size, sizeof(uint64_t), true);
}

uint32_t Updater::CalculateMaxIndexPRSize() {
  uint32_t index_pr_size = 0;
  auto index_oids = exec_ctx_->GetAccessor()->GetIndexOids(table_oid_);
  for (auto index_oid : index_oids) {
    auto pri = GetIndex(index_oid)->GetProjectedRowInitializer();
    index_pr_size = std::max(index_pr_size, pri.ProjectedRowSize());
  }
  return index_pr_size;
}

void Updater::PopulateAllColOids(std::vector<catalog::col_oid_t> *all_col_oids) {
  auto columns = exec_ctx_->GetAccessor()->GetSchema(table_oid_).GetColumns();
  for (const auto &col : columns) {
    all_col_oids->push_back(col.Oid());
  }
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
  if (is_index_key_update_) {
    return GetTablePRForColumns(all_col_oids_);
  }
  return GetTablePRForColumns(col_oids_);
}

storage::ProjectedRow *Updater::GetTablePRForColumns(const std::vector<catalog::col_oid_t> &col_oids) {
  TERRIER_ASSERT(!col_oids.empty(), "Should have at least one column oid");
  storage::ProjectedRowInitializer pri = table_->InitializerForProjectedRow(col_oids);
  auto txn = exec_ctx_->GetTxn();
  table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri);
  return table_redo_->Delta();
}

storage::ProjectedRow *Updater::GetIndexPR(catalog::index_oid_t index_oid) {
  TERRIER_ASSERT(is_index_key_update_, "Should not be called as index key is not being updated");
  auto index = GetIndex(index_oid);
  index_pr_ = index->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

bool Updater::TableUpdate(storage::TupleSlot table_tuple_slot) {
  TERRIER_ASSERT(!is_index_key_update_, "Should not be called as index key is being updated");
  table_redo_->SetTupleSlot(table_tuple_slot);
  return table_->Update(exec_ctx_->GetTxn(), table_redo_);
}

bool Updater::TableDelete(storage::TupleSlot table_tuple_slot) {
  TERRIER_ASSERT(is_index_key_update_, "Should not be called as index key is not being updated");
  auto txn = exec_ctx_->GetTxn();
  txn->StageDelete(exec_ctx_->DBOid(), table_oid_, table_tuple_slot);
  return table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
}

storage::TupleSlot Updater::TableInsert() {
  TERRIER_ASSERT(is_index_key_update_, "Should not be called as index key is not being updated");
  auto table_tuple_slot = table_->Insert(exec_ctx_->GetTxn(), table_redo_);
  return table_tuple_slot;
}

bool Updater::IndexInsert(catalog::index_oid_t index_oid) {
  TERRIER_ASSERT(is_index_key_update_, "Should not be called as index key is not being updated");
  auto index = GetIndex(index_oid);
  return index->Insert(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}

void Updater::IndexDelete(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot) {
  TERRIER_ASSERT(is_index_key_update_, "Should not be called as index key is not being updated");
  auto index = GetIndex(index_oid);
  index->Delete(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}
}  // namespace terrier::execution::sql
