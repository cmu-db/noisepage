#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {
class ThreadStateContainer;

/**
 * An iterator over a table's data in vector-wise fashion.
 * TODO(Amadou): Add a Reset() method to avoid reconstructing the object in NL joins.
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
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /**
   * Initialize the iterator, returning true if the initialization succeeded
   * @return True if the initialization succeeded; false otherwise
   */
  bool Init();

  /**
   * Advance the iterator by a vector of input
   * @return True if there is more data in the iterator; false otherwise
   */
  bool Advance();

  /**
   * @return the iterator over the current active projection
   */
  ProjectedColumnsIterator *GetProjectedColumnsIterator() { return &pci_; }

  /**
   * Scan function callback used to scan a partition of the table.
   * Convention: First argument is the opaque query state, second argument is
   *             the thread state, and last argument is the table vector
   *             iterator configured to iterate a sub-range of the table. The
   *             first two arguments are void because their types are only known
   *             at runtime (i.e., defined in generated code).
   */
  using ScanFn = void (*)(void *, void *, TableVectorIterator *iter);

  /**
   * Perform a parallel scan over the table with ID @em table_oid using the
   * callback function @em scanner on each input vector projection from the
   * source table. This call is blocking, meaning that it only returns after
   * the whole table has been scanned. Iteration order is non-deterministic.
   * @param db_oid The ID of the database containing the table
   * @param table_oid The ID of the table
   * @param query_state the query state
   * @param thread_states the thread state container
   * @param scan_fn The callback function invoked for vectors of table input
   * @param min_grain_size The minimum number of blocks to give a scan task
   */
  static bool ParallelScan(uint32_t db_oid, uint32_t table_oid, void *query_state, ThreadStateContainer *thread_states,
                           ScanFn scan_fn, uint32_t min_grain_size = K_MIN_BLOCK_RANGE_SIZE);

 private:
  exec::ExecutionContext *exec_ctx_;
  const catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> col_oids_{};
  // The PCI
  ProjectedColumnsIterator pci_;
  // SqlTable to iterate over
  common::ManagedPointer<storage::SqlTable> table_{nullptr};
  // A PC and its buffer.
  void *buffer_ = nullptr;
  storage::ProjectedColumns *projected_columns_ = nullptr;
  // Iterator of the slots in the PC
  std::unique_ptr<storage::DataTable::SlotIterator> iter_ = nullptr;

  bool initialized_ = false;
};

}  // namespace terrier::execution::sql
