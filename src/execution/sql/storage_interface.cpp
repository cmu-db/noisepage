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
      has_table_pr_(false),
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
  if (has_table_pr_) exec_ctx_->GetMemoryPool()->Deallocate(table_pr_buffer_, table_pr_size_);
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
    index_pr_buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(
        curr_index_->GetProjectedRowInitializer().ProjectedRowSize(), alignof(uint64_t), false);
    need_indexes_ = true;
  }
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

storage::ProjectedRow *StorageInterface::InitTablePR(catalog::index_oid_t index_oid) {
  const auto &indexed_attributes = exec_ctx_->GetAccessor()->GetIndexSchema(index_oid).GetIndexedColOids();
  const auto table_pr_initializer = table_->InitializerForProjectedRow(indexed_attributes);
  table_pr_size_ = table_pr_initializer.ProjectedRowSize();
  table_pr_buffer_ = common::AllocationUtil::AllocateAligned(table_pr_size_);
  table_pr_ = table_pr_initializer.InitializeRow(table_pr_buffer_);
  has_table_pr_ = true;
  index_oid_ = index_oid;
  return table_pr_;
}

bool StorageInterface::FillTablePR(storage::TupleSlot table_tuple_slot) {
  // TODO(Wuwen): Move it to a better place

  table_->Select(exec_ctx_->GetTxn(), table_tuple_slot, table_pr_);
  const auto key_schema = exec_ctx_->GetAccessor()->GetIndexSchema(index_oid_);
  const auto &indexed_attributes = key_schema.GetIndexedColOids();
  auto pr_map = table_->ProjectionMapForOids(indexed_attributes);

  auto num_index_cols = key_schema.GetColumns().size();
  TERRIER_ASSERT(num_index_cols == indexed_attributes.size(), "Only support index keys that are a single column oid");
  for (uint32_t col_idx = 0; col_idx < num_index_cols; col_idx++) {
    const auto &col = key_schema.GetColumn(col_idx);
    auto index_col_oid = col.Oid();
    const catalog::col_oid_t &table_col_oid = indexed_attributes[col_idx];
    if (table_pr_->IsNull(pr_map[table_col_oid])) {
      index_pr_->SetNull(curr_index_->GetKeyOidToOffsetMap().at(index_col_oid));
    } else {
      // TODO(Wuwen): This may not be thread safe
      auto size = storage::AttrSizeBytes(col.AttrSize());
      std::memcpy(index_pr_->AccessForceNotNull(curr_index_->GetKeyOidToOffsetMap().at(index_col_oid)),
                  table_pr_->AccessWithNullCheck(pr_map[table_col_oid]), size);
    }
  }

  return curr_index_->Insert(exec_ctx_->GetTxn(), *index_pr_, table_tuple_slot);
}

}  // namespace terrier::execution::sql
