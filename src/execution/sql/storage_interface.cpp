#include "execution/sql/storage_interface.h"

#include <algorithm>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

StorageInterface::StorageInterface(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                                   uint32_t num_oids, bool need_indexes)
    : table_oid_{table_oid},
      table_(exec_ctx->GetAccessor()->GetTable(table_oid)),
      exec_ctx_(exec_ctx),
      col_oids_(col_oids, col_oids + num_oids),
      need_indexes_(need_indexes) {
  // Initialize the index projected row if needed.
  if (need_indexes_) {
    // Get index pr size
    max_pr_size_ = 0;
    auto index_oids = exec_ctx->GetAccessor()->GetIndexOids(table_oid);
    for (auto index_oid : index_oids) {
      auto index_ptr = exec_ctx->GetAccessor()->GetIndex(index_oid);
      max_pr_size_ = std::max(max_pr_size_, index_ptr->GetProjectedRowInitializer().ProjectedRowSize());
    }
    // Allocate pr buffer.
    index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(max_pr_size_, alignof(uint64_t), false);
  }
}

StorageInterface::~StorageInterface() {
  if (need_indexes_) exec_ctx_->GetMemoryPool()->Deallocate(index_pr_buffer_, max_pr_size_);
}

storage::ProjectedRow *StorageInterface::GetTablePR() {
  // We need all the columns
  storage::ProjectedRowInitializer pri = table_->InitializerForProjectedRow(col_oids_);
  auto txn = exec_ctx_->GetTxn();
  table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri);
  return table_redo_->Delta();
}

storage::ProjectedRow *StorageInterface::GetIndexPR(catalog::index_oid_t index_oid) {
  curr_index_ = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  index_pr_ = curr_index_->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

storage::TupleSlot StorageInterface::TableInsert() { return table_->Insert(exec_ctx_->GetTxn(), table_redo_); }

bool StorageInterface::TableDelete(storage::TupleSlot table_tuple_slot) {
  auto txn = exec_ctx_->GetTxn();
  txn->StageDelete(exec_ctx_->DBOid(), table_oid_, table_tuple_slot);
  return table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
}

bool StorageInterface::TableUpdate(storage::TupleSlot table_tuple_slot) {
  table_redo_->SetTupleSlot(table_tuple_slot);
  return table_->Update(exec_ctx_->GetTxn(), table_redo_);
}

bool StorageInterface::IndexInsert() {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  return curr_index_->Insert(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}

void StorageInterface::IndexDelete(storage::TupleSlot table_tuple_slot) {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  curr_index_->Delete(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}

}  // namespace terrier::execution::sql
