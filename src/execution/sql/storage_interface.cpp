#include "execution/sql/storage_interface.h"

#include <algorithm>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {

StorageInterface::StorageInterface(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *col_oids,
                                   uint32_t num_oids, bool need_indexes)
    : table_oid_{table_oid},
      table_(exec_ctx->GetAccessor()->GetTable(table_oid)),
      exec_ctx_(exec_ctx),
      col_oids_(col_oids, col_oids + num_oids),
      need_indexes_(need_indexes),
      pri_(num_oids > 0 ? table_->InitializerForProjectedRow(col_oids_) : storage::ProjectedRowInitializer()) {
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
  auto txn = exec_ctx_->GetTxn();
  table_redo_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, pri_);
  return table_redo_->Delta();
}

storage::ProjectedRow *StorageInterface::GetIndexPR(catalog::index_oid_t index_oid) {
  curr_index_ = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  // index is created after the initialization of storage interface
  if (curr_index_ != nullptr && !need_indexes_) {
    max_pr_size_ = curr_index_->GetProjectedRowInitializer().ProjectedRowSize();
    index_pr_buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(max_pr_size_, alignof(uint64_t), false);
    need_indexes_ = true;
  }
  index_pr_ = curr_index_->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

storage::TupleSlot StorageInterface::TableInsert() { return table_->Insert(exec_ctx_->GetTxn(), table_redo_); }

uint32_t StorageInterface::GetIndexHeapSize() {
  TERRIER_ASSERT(curr_index_ != nullptr, "Index must have been loaded");
  return curr_index_->EstimateHeapUsage();
}

bool StorageInterface::TableDelete(storage::TupleSlot table_tuple_slot) {
  auto txn = exec_ctx_->GetTxn();
  txn->StageDelete(exec_ctx_->DBOid(), table_oid_, table_tuple_slot);
  return table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
}

bool StorageInterface::TableUpdate(storage::TupleSlot table_tuple_slot) {
  table_redo_->SetTupleSlot(table_tuple_slot);
  return table_->Update(exec_ctx_->GetTxn(), table_redo_);
}

uint64_t StorageInterface::IndexGetSize() const { return curr_index_->GetSize(); }

bool StorageInterface::IndexInsert() {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  return curr_index_->Insert(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}

bool StorageInterface::IndexInsertUnique() {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  return curr_index_->InsertUnique(exec_ctx_->GetTxn(), *index_pr_, table_redo_->GetTupleSlot());
}

void StorageInterface::IndexDelete(storage::TupleSlot table_tuple_slot) {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  curr_index_->Delete(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}

bool StorageInterface::IndexInsertWithTuple(storage::TupleSlot table_tuple_slot, bool unique) {
  TERRIER_ASSERT(need_indexes_, "Index PR not allocated!");
  if (unique) {
    return curr_index_->InsertUnique(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
  }
  return curr_index_->Insert(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}

}  // namespace terrier::execution::sql
