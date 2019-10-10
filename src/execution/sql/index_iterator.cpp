#include "execution/sql/index_iterator.h"
#include "execution/sql/value.h"

namespace terrier::execution::sql {

IndexIterator::IndexIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t index_oid,
                             uint32_t *col_oids, uint32_t num_oids)
    : exec_ctx_(exec_ctx),
      col_oids_(col_oids, col_oids + num_oids),
      index_(exec_ctx_->GetAccessor()->GetIndex(catalog::index_oid_t(index_oid))),
      table_(exec_ctx_->GetAccessor()->GetTable(catalog::table_oid_t(table_oid))) {}

void IndexIterator::Init() {
  // Initialize projected rows for the index and the table
  TERRIER_ASSERT(!col_oids_.empty(), "There must be at least one col oid!");
  // Table's PR
  auto table_pri = table_->InitializerForProjectedRow(col_oids_);
  table_buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(table_pri.ProjectedRowSize(), alignof(uint64_t), false);
  table_pr_ = table_pri.InitializeRow(table_buffer_);

  // Index's PR
  auto &index_pri = index_->GetProjectedRowInitializer();
  index_buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(index_pri.ProjectedRowSize(), alignof(uint64_t), false);
  index_pr_ = index_pri.InitializeRow(index_buffer_);
}

void IndexIterator::ScanKey() {
  // Scan the index
  tuples_.clear();
  curr_index_ = 0;
  index_->ScanKey(*exec_ctx_->GetTxn(), *index_pr_, &tuples_);
}

bool IndexIterator::Advance() {
  if (curr_index_ < tuples_.size()) {
    table_->Select(exec_ctx_->GetTxn(), tuples_[curr_index_], table_pr_);
    ++curr_index_;
    return true;
  }
  return false;
}

IndexIterator::~IndexIterator() {
  // Free allocated buffers
  exec_ctx_->GetMemoryPool()->Deallocate(table_buffer_, table_pr_->Size());
  exec_ctx_->GetMemoryPool()->Deallocate(index_buffer_, index_pr_->Size());
}
}  // namespace terrier::execution::sql
