#include "execution/sql/table_vector_iterator.h"

#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/sql/column_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/timer.h"
#include "tbb/parallel_for.h"
#include "tbb/task_scheduler_init.h"

namespace terrier::execution::sql {

TableVectorIterator::TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids,
                                         uint32_t num_oids)
    : exec_ctx_(exec_ctx), table_oid_(table_oid), col_oids_(col_oids, col_oids + num_oids) {}

TableVectorIterator::~TableVectorIterator() {
  exec_ctx_->GetMemoryPool()->Deallocate(vp_buffer_, sizeof(VectorProjection));
}

bool TableVectorIterator::Init() {
  // No-op if already initialized
  if (IsInitialized()) {
    return true;
  }

  // Set up the table and the iterator.
  table_ = exec_ctx_->GetAccessor()->GetTable(table_oid_);
  TERRIER_ASSERT(table_ != nullptr, "Table must exist!!");
  iter_ = std::make_unique<storage::DataTable::SlotIterator>(table_->begin());
  const auto &table_col_map = table_->GetColumnMap();

  // Configure the vector projection, create the column iterators.
  std::vector<storage::col_id_t> col_ids;
  std::vector<TypeId> col_types(col_oids_.size());
  column_iterators_.reserve(col_oids_.size());
  for (uint64_t idx = 0; idx < col_oids_.size(); idx++) {
    auto col_oid = col_oids_[idx];
    auto col_type = GetTypeId(table_col_map.at(col_oid).col_type_);
    auto storage_col_id = table_col_map.at(col_oid).col_id_;

    col_ids.emplace_back(storage_col_id);
    col_types[idx] = col_type;
    column_iterators_.emplace_back(GetTypeIdSize(col_type));
  }

  // Create a referencing vector.
  vp_buffer_ = exec_ctx_->GetMemoryPool()->AllocateAligned(sizeof(VectorProjection), alignof(uint64_t), false);
  vector_projection_ = new (vp_buffer_) VectorProjection();
  vector_projection_->SetStorageColIds(col_ids);
  vector_projection_->InitializeEmpty(col_types);

  // All good.
  initialized_ = true;
  return true;
}

void TableVectorIterator::RefreshVectorProjection() {
  // Reset our projection and refresh all columns with new data from the column iterators.

  const uint32_t tuple_count = column_iterators_[0].GetTupleCount();

  TERRIER_ASSERT(std::all_of(column_iterators_.begin(), column_iterators_.end(),
                             [&](const auto &iter) { return tuple_count == iter.GetTupleCount(); }),
                 "Not all iterators have the same size?");

  vector_projection_->Reset(tuple_count);
  for (uint64_t col_idx = 0; col_idx < column_iterators_.size(); col_idx++) {
    Vector *column_vector = vector_projection_->GetColumn(col_idx);
    column_vector->Reference(column_iterators_[col_idx].GetColumnData(),
                             column_iterators_[col_idx].GetColumnNullBitmap(),
                             column_iterators_[col_idx].GetTupleCount());
  }
  vector_projection_->CheckIntegrity();

  // Insert our vector projection instance into the vector projection iterator.
  vector_projection_iterator_.SetVectorProjection(vector_projection_);
}

bool TableVectorIterator::Advance() {
  // Cannot advance if not initialized.
  if (!IsInitialized()) {
    return false;
  }

  // If the iterator is out of data, then we are done.
  if (*iter_ == table_->end()) {
    return false;
  }

  // Otherwise, scan the table to set the vector projection.
  table_->Scan(exec_ctx_->GetTxn(), iter_.get(), vector_projection_);
  vector_projection_iterator_.SetVectorProjection(vector_projection_);

  return true;
}

/*
TODO(WAN): wait until PR merged for parallel scan interface to blocks
namespace {

class ScanTask {
 public:
  ScanTask(uint16_t table_id, void *const query_state, ThreadStateContainer *const thread_state_container,
           TableVectorIterator::ScanFn scanner)
      : table_id_(table_id),
        query_state_(query_state),
        thread_state_container_(thread_state_container),
        scanner_(scanner) {}

  void operator()(const tbb::blocked_range<uint32_t> &block_range) const {
    // Create the iterator over the specified block range
    TableVectorIterator iter(table_id_, block_range.begin(), block_range.end());

    // Initialize it
    if (!iter.Init()) {
      return;
    }

    // Pull out the thread-local state
    byte *const thread_state = thread_state_container_->AccessCurrentThreadState();

    // Call scanning function
    scanner_(query_state_, thread_state, &iter);
  }

 private:
  uint16_t table_id_;
  void *const query_state_;
  ThreadStateContainer *const thread_state_container_;
  TableVectorIterator::ScanFn scanner_;
};

}  // namespace

bool TableVectorIterator::ParallelScan(const uint16_t table_id, void *const query_state,
                                       ThreadStateContainer *const thread_states,
                                       const TableVectorIterator::ScanFn scan_fn, const uint32_t min_grain_size) {
  // Lookup table
  const Table *table = Catalog::Instance()->LookupTableById(table_id);
  if (table == nullptr) {
    return false;
  }

  // Time
  util::Timer<std::milli> timer;
  timer.Start();

  // Execute parallel scan
  tbb::task_scheduler_init scan_scheduler;
  tbb::blocked_range<uint32_t> block_range(0, table->GetBlockCount(), min_grain_size);
  tbb::parallel_for(block_range, ScanTask(table_id, query_state, thread_states, scan_fn));

  timer.Stop();

  double tps = table->GetTupleCount() / timer.GetElapsed() / 1000.0;
  LOG_INFO("Scanned {} blocks ({} tuples) in {} ms ({:.3f} mtps)", table->GetBlockCount(), table->GetTupleCount(),
           timer.GetElapsed(), tps);

  return true;
}
*/
}  // namespace terrier::execution::sql
