#include "execution/sql/storage_interface.h"

#include <storage/index/index_builder.h>

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
  index_pr_ = curr_index_->GetProjectedRowInitializer().InitializeRow(index_pr_buffer_);
  return index_pr_;
}

storage::TupleSlot StorageInterface::TableInsert() {
  exec_ctx_->RowsAffected()++;  // believe this should only happen in root plan nodes, so should reflect count of query
  return table_->Insert(exec_ctx_->GetTxn(), table_redo_);
}

bool StorageInterface::TableDelete(storage::TupleSlot table_tuple_slot) {
  exec_ctx_->RowsAffected()++;  // believe this should only happen in root plan nodes, so should reflect count of query
  auto txn = exec_ctx_->GetTxn();
  txn->StageDelete(exec_ctx_->DBOid(), table_oid_, table_tuple_slot);
  return table_->Delete(exec_ctx_->GetTxn(), table_tuple_slot);
}

bool StorageInterface::TableUpdate(storage::TupleSlot table_tuple_slot) {
  exec_ctx_->RowsAffected()++;  // believe this should only happen in root plan nodes, so should reflect count of query
  table_redo_->SetTupleSlot(table_tuple_slot);
  return table_->Update(exec_ctx_->GetTxn(), table_redo_);
}

catalog::index_oid_t StorageInterface::IndexCreate(catalog::namespace_oid_t ns, std::string index_name,
                                                   const catalog::IndexSchema &schema) {
  auto index_oid = exec_ctx_->GetAccessor()->CreateIndex(ns, table_oid_, std::move(index_name), schema);
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(schema);
  auto *const index = index_builder.Build();
  bool result UNUSED_ATTRIBUTE = exec_ctx_->GetAccessor()->SetIndexPointer(index_oid, index);
  if (index_oid == catalog::INVALID_INDEX_OID) {
    exec_ctx_->GetTxn()->SetMustAbort();
    TERRIER_ASSERT(index_oid != catalog::INVALID_INDEX_OID, "invalid index oid");
  }
  return index_oid;
}

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

bool StorageInterface::IndexCreateInsert(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot) {
  terrier::storage::index::IndexBuilder index_builder;
  index_builder.SetSqlTableAndTransactionContext(exec_ctx_->GetTxn(), table_,
                                                 exec_ctx_->GetAccessor()->GetIndexSchema(index_oid));
  auto result =  index_builder.Insert(exec_ctx_->GetAccessor()->GetIndex(index_oid), table_tuple_slot);
  exec_ctx_->GetAccessor()->SetIndexLive(index_oid);
  return result;
}

}  // namespace terrier::execution::sql
