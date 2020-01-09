#include <memory>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"

namespace terrier::execution::sql {
TableVectorIterator::TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids,
                                         uint32_t num_oids)
    : exec_ctx_(exec_ctx), table_oid_(table_oid), col_oids_(col_oids, col_oids + num_oids) {}

TableVectorIterator::~TableVectorIterator() {
  exec_ctx_->GetMemoryPool()->Deallocate(buffer_, projected_columns_->Size());
}

bool TableVectorIterator::Init() {
  // Find the table
  table_ = exec_ctx_->GetAccessor()->GetTable(table_oid_);
  TERRIER_ASSERT(table_ != nullptr, "Table must exist!!");

  // Initialize the projected column
  TERRIER_ASSERT(!col_oids_.empty(), "There must be at least one col oid!");
  auto pc_init = table_->InitializerForProjectedColumns(col_oids_, common::Constants::K_DEFAULT_VECTOR_SIZE);
  buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(pc_init.ProjectedColumnsSize(), alignof(uint64_t), false);
  projected_columns_ = pc_init.Initialize(buffer_);
  initialized_ = true;

  // Begin iterating
  iter_ = std::make_unique<storage::DataTable::SlotIterator>(table_->begin());
  return true;
}

bool TableVectorIterator::Advance() {
  if (!initialized_) return false;
  // First check if the iterator ended.
  if (*iter_ == table_->end()) {
    return false;
  }
  // Scan the table to set the projected column.
  table_->Scan(exec_ctx_->GetTxn(), iter_.get(), projected_columns_);
  pci_.SetProjectedColumn(projected_columns_);
  return true;
}

void TableVectorIterator::Reset() {
  if (!initialized_) return;
  iter_ = std::make_unique<storage::DataTable::SlotIterator>(table_->begin());
}

bool TableVectorIterator::ParallelScan(uint32_t db_oid, uint32_t table_oid, void *const query_state,
                                       ThreadStateContainer *const thread_states, const ScanFn scan_fn,
                                       const uint32_t min_grain_size) {
  // TODO(Amadou): Implement Me!!
  return false;
}

}  // namespace terrier::execution::sql
