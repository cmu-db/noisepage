#pragma once

#include <memory>
#include <vector>

#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"
#include "storage/sql_table.h"

namespace terrier::execution::exec {
class ExecutionContext;
}

namespace terrier::storage {
class SqlTable;
}

namespace terrier::execution::sql {

class ThreadStateContainer;

/**
 * An iterator over a table's data in vector-wise fashion.
 */
class EXPORT TableVectorIterator {
 public:
  /**
   * Minimum block range
   */
  static constexpr const uint32_t K_MIN_BLOCK_RANGE_SIZE = 2;

  /**
   * Create a new vectorized iterator over the given table
   * @param exec_ctx execution context of the query
   * @param table_oid oid of the table
   * @param col_oids array column oids to scan
   * @param num_oids length of the array
   */
  explicit TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids,
                               uint32_t num_oids);

  /**
   * Destructor
   */
  ~TableVectorIterator();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /**
   * Initialize the iterator, returning true if the initialization succeeded.
   * @return True if the initialization succeeded; false otherwise.
   */
  bool Init();

  bool Init(uint32_t block_start, uint32_t block_end);

  /**
   * Advance the iterator by a vector of input.
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool Advance();

  /**
   * @return True if the iterator has been initialized; false otherwise.
   */
  bool IsInitialized() const { return initialized_; }

  /**
   * @return The iterator over the current active vector projection.
   */
  VectorProjectionIterator *GetVectorProjectionIterator() { return &vector_projection_iterator_; }

  /**
   * Scan function callback used to scan a partition of the table.
   * Convention: First argument is the opaque query state, second argument is the thread state,
   *             third argument is the table vector iterator configured to iterate a sub-range of
   *             the table, fourth argument is the execution context. The first two arguments are void because their types are only known at
   *             runtime (i.e., defined in generated code).
   */
  using ScanFn = void (*)(void *, void *, TableVectorIterator *iter, exec::ExecutionContext *);

  /**
   * Perform a parallel scan over the table with ID @em table_id using the callback function
   * @em scanner on each input vector projection from the source table. This call is blocking,
   * meaning that it only returns after the whole table has been scanned. Iteration order is
   * non-deterministic.
   * @param table_id The ID of the table to scan.
   * @param col_oids The column OIDs of the table to scan.
   * @param num_oids The number of column OIDs provided in col_oids.
   * @param query_state An opaque pointer to some query-specific state. Passed to scan functions.
   * @param exec_ctx The execution context to run the parallel scan in. It should point to a
   *                 ThreadStateContainer for all thread states, where it is assumed that the
   *                 container has been configured for size, construction, and destruction
   *                 before this invocation.
   * @param thread_states Container for all thread states. It's assumed that the container has been
   *                      configured for size, construction, and destruction before this invocation.
   * @param scan_fn The callback function invoked for vectors of table input.
   * @param min_grain_size The minimum number of blocks to give a scan task.
   */
  static bool ParallelScan(uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids,
                           void *query_state, exec::ExecutionContext *exec_ctx, ScanFn scan_fn,
                           uint32_t min_grain_size = K_MIN_BLOCK_RANGE_SIZE);

  /** When the column iterators receive new vectors of input, we need to refresh the projection with new data too. */
  void RefreshVectorProjection();

 private:
  exec::ExecutionContext *exec_ctx_;
  const catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> col_oids_{};

  // The SqlTable to iterate over.
  common::ManagedPointer<storage::SqlTable> table_{nullptr};

  // The vector-wise iterators over each column in the table.
  std::vector<ColumnVectorIterator> column_iterators_;

  std::unique_ptr<storage::DataTable::SlotIterator> iter_ = nullptr;

  // TODO(WAN): Matt, does this have to be done for memory tracking reasons?
  // A vector projection and its buffer.
  void *vp_buffer_ = nullptr;
  VectorProjection *vector_projection_ = nullptr;

  // An iterator over the currently active projection.
  VectorProjectionIterator vector_projection_iterator_;

  // True if the iterator has been initialized.
  bool initialized_{false};
};

}  // namespace terrier::execution::sql
