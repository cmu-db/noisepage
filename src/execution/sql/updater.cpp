
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

#include "execution/sql/updater.h"

namespace terrier::execution::sql {

Updater::Updater(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                 uint32_t num_oids)
    : table_oid_(table_oid), col_oids_(col_oids, col_oids + num_oids), exec_ctx_(exec_ctx) {
  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);

  // We need this to calculate the max sized index pr
  uint32_t index_pr_size = 0;
  auto index_oids = exec_ctx->GetAccessor()->GetIndexes(table_oid_);
  for (auto index_oid : index_oids) {
    auto pri = GetIndex(index_oid)->GetProjectedRowInitializer();

    // Calculate the max sized index pr
    index_pr_size = std::max(index_pr_size, pri.ProjectedRowSize());

    // We don't need to do anything if we already know that we have to update index
    if (!is_index_key_update_) {
      // If there is an index key that is being updated
      auto num_cols_in_index = pri.NumColumns();

      // Loop through every column for that index and see if that column is being updated
      for (auto pri_col_idx = 0; pri_col_idx < num_cols_in_index; pri_col_idx += 1) {
        auto is_index_key = std::find(col_oids_.begin(), col_oids_.end(), pri.ColId(pri_col_idx)) != col_oids_.end();
        if (is_index_key) {
          is_index_key_update_ = true;
          break;
        }
      }
    }
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

storage::TupleSlot Updater::TableUpdate(storage::TupleSlot table_tuple_slot) {
  if (is_index_key_update_) {
    // If any index key is modified, we have to Delete and Insert from the table.
    table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
    table_->Insert(exec_ctx_->GetTxn(), table_redo_);
    return table_redo_->GetTupleSlot();
  }

  table_redo_->SetTupleSlot(table_tuple_slot);
  table_->Update(exec_ctx_->GetTxn(), table_redo_);
  return table_tuple_slot;
}

bool Updater::IndexInsert(catalog::index_oid_t index_oid) {
  if (is_index_key_update_) {
    auto index = GetIndex(index_oid);
    return index->Insert(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
  }
  return false;
}

void Updater::IndexDelete(catalog::index_oid_t index_oid) {
  if (is_index_key_update_) {
    auto index = GetIndex(index_oid);
    index->Delete(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
  }
}
}  // namespace terrier::execution::sql
