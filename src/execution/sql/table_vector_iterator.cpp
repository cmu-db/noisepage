#include "execution/sql/table_vector_iterator.h"

#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>
#include <iostream>
#include <memory>
#include <thread>  //NOLINT

#include "execution/exec/execution_context.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql {
TableVectorIterator::TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids,
                                         uint32_t num_oids)
    : TableVectorIterator(exec_ctx, table_oid, col_oids, num_oids, 0, std::numeric_limits<uint32_t>::max()) {}

TableVectorIterator::TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids,
                                         uint32_t num_oids, uint32_t start_block_idx, uint32_t end_block_idx)
    : exec_ctx_(exec_ctx),
      table_oid_(table_oid),
      col_oids_(col_oids, col_oids + num_oids),
      start_block_idx_(start_block_idx),
      end_block_idx_(end_block_idx) {}

TableVectorIterator::~TableVectorIterator() {
  exec_ctx_->GetMemoryPool()->Deallocate(buffer_, projected_columns_->Size());
}

bool TableVectorIterator::Init() {
  // Find the table
  table_ = exec_ctx_->GetAccessor()->GetTable(table_oid_);
  TERRIER_ASSERT(table_ != nullptr, "Table must exist!!");
  if (col_oids_.empty()) {
    table_->GetAllColOid(&col_oids_);
  }
  // Initialize the projected column
  // TERRIER_ASSERT(!col_oids_.empty(), "There must be at least one col oid!");
  auto pc_init = table_->InitializerForProjectedColumns(col_oids_, common::Constants::K_DEFAULT_VECTOR_SIZE);
  buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(pc_init.ProjectedColumnsSize(), alignof(uint64_t), false);
  projected_columns_ = pc_init.Initialize(buffer_);
  initialized_ = true;

  // Create the start and end iterator
  iter_ = std::make_unique<storage::DataTable::SlotIterator>(table_->beginAt(start_block_idx_));
  iter_end_ = std::make_unique<storage::DataTable::SlotIterator>(table_->endAt(end_block_idx_));
  return true;
}

bool TableVectorIterator::Advance() {
  if (!initialized_) return false;
  // First check if the iterator ended.
  if (*iter_ == *iter_end_) {
    return false;
  }
  // Scan the table to set the projected column.
  table_->RangeScan(exec_ctx_->GetTxn(), iter_.get(), iter_end_.get(), projected_columns_);
  pci_.SetProjectedColumn(projected_columns_);
  return true;
}

void TableVectorIterator::Reset() {
  if (!initialized_) return;
  // set the iterator to the start position specified by tbb
  iter_ = std::make_unique<storage::DataTable::SlotIterator>(table_->beginAt(start_block_idx_));
}

namespace {

class ScanTask {
 public:
  ScanTask(exec::ExecutionContext *exec_ctx, uint16_t table_id, uint32_t *col_oids, uint32_t num_oids,
           void *const query_state, TableVectorIterator::ScanFn scanner)
      : exec_ctx_(exec_ctx),
        table_id_(table_id),
        col_oids_(col_oids),
        num_oids_(num_oids),
        query_state_(query_state),
        scanner_(scanner) {}

  void operator()(const tbb::blocked_range<uint32_t> &block_range) const {
    // Create the iterator over the specified block range
    TableVectorIterator iter(exec_ctx_, table_id_, col_oids_, num_oids_, block_range.begin(), block_range.end());

    // Initialize the table vector iterator
    if (!iter.Init()) {
      return;
    }

    // Call scanning function which should be passed at runtime
    scanner_(query_state_, exec_ctx_, &iter);
  }

 private:
  exec::ExecutionContext *exec_ctx_;
  uint16_t table_id_;
  uint32_t *col_oids_;
  uint32_t num_oids_;
  void *const query_state_;
  TableVectorIterator::ScanFn scanner_;
};
}  // namespace

bool TableVectorIterator::ParallelScan(uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids,
                                       void *query_state, const ScanFn scan_fn,
                                       exec::ExecutionContext *exec_ctx) {
  // Lookup table
  common::ManagedPointer<storage::SqlTable> table =
      exec_ctx->GetAccessor()->GetTable(static_cast<catalog::table_oid_t>(table_oid));
  if (table == nullptr) {
    return false;
  }

  // Get number of cores
  auto processor_count = std::thread::hardware_concurrency();
  if (processor_count == 0) {
    // Single thread if fail to get the number of cores
    processor_count = 1;
  }

  // Get the number of blocks in the table
  auto block_count = table->GetBlockListSize();

  // Calculate number of blocks for each thread
  size_t min_grain_size =
      (table->GetBlockListSize() / processor_count) + ((table->GetBlockListSize() % processor_count) > 0 ? 1 : 0);

  // Execute parallel scan
  tbb::task_scheduler_init scan_scheduler;
  // partition the block list
  tbb::blocked_range<uint32_t> block_range(0, block_count, min_grain_size);
  // invoke parallel scan for multiple workers
  tbb::parallel_for(block_range, ScanTask(exec_ctx, table_oid, col_oids, num_oids, query_state, scan_fn));

  return true;
}

}  // namespace terrier::execution::sql
