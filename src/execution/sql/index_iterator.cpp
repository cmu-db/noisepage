#include "execution/sql/index_iterator.h"
#include "execution/sql/value.h"

namespace tpl::sql {

IndexIterator::IndexIterator(uint32_t table_oid, uint32_t index_oid, exec::ExecutionContext *exec_ctx)
    : exec_ctx_(exec_ctx)
    , index_(exec_ctx_->GetAccessor()->GetIndex(terrier::catalog::index_oid_t(index_oid)))
    , table_(exec_ctx_->GetAccessor()->GetTable(terrier::catalog::table_oid_t(table_oid)))
    , schema_(exec_ctx_->GetAccessor()->GetSchema(terrier::catalog::table_oid_t(table_oid))){

}

void IndexIterator::Init() {
  // Initialize projected rows for the index and the table
  if (col_oids_.empty()) {
    // If no col_oid is passed in read all columns.
    for (const auto & col : schema_.GetColumns()) {
      col_oids_.emplace_back(col.Oid());
    }
  }
  auto pri_map = table_->InitializerForProjectedRow(col_oids_);
  // Table's PR
  auto &table_pri = pri_map.first;
  table_buffer_ = terrier::common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  table_pr_ = table_pri.InitializeRow(table_buffer_);

  // Index's PR
  auto &index_pri = index_->GetProjectedRowInitializer();
  index_buffer_ = terrier::common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
  index_pr_ = index_pri.InitializeRow(index_buffer_);
}

IndexIterator::~IndexIterator() {
  // Free allocated buffers
  delete[] index_buffer_;
  delete[] table_buffer_;
}
}  // namespace tpl::sql
