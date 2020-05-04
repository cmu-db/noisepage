#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/sql/thread_state_container.h"
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
   * Create a new vectorized iterator over the given table block range [start, end)
   * @param exec_ctx execution context of the query
   * @param table_oid oid of the table
   * @param col_oids array column oids to scan
   * @param num_oids length of the array
   * @param start_block_idx start block index to scan
   * @param end_block_idx end block index to scan
   */
  TableVectorIterator(exec::ExecutionContext *exec_ctx, uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids,
                      uint32_t start_block_idx, uint32_t end_block_idx);
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
   * Reset the iterator.
   */
  void Reset();

  /**
   * @return the iterator over the current active projection
   */
  ProjectedColumnsIterator *GetProjectedColumnsIterator() { return &pci_; }

  /**
   * Scan function callback used to scan a partition of the table.
   * Convention: First argument is the opaque query state, second argument is
   *             the execution context, and last argument is the table vector
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
   * @param table_oid The ID of the table
   * @param col_oids The columns to be retrieved
   * @param num_oids Number of columns
   * @param query_state the query state
   * @param scan_fn The callback function invoked for vectors of table input
   * @param exec_ctx Current execution context
   */
  static bool ParallelScan(uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids, void *query_state, ScanFn scan_fn,
                           exec::ExecutionContext *exec_ctx);

 private:
  exec::ExecutionContext *exec_ctx_;
  const catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> col_oids_{};
  uint32_t start_block_idx_;
  uint32_t end_block_idx_;
  // The PCI
  ProjectedColumnsIterator pci_;
  // SqlTable to iterate over
  common::ManagedPointer<storage::SqlTable> table_{nullptr};
  // A PC and its buffer.
  void *buffer_ = nullptr;
  storage::ProjectedColumns *projected_columns_ = nullptr;
  // Iterator of the slots in the PC
  std::unique_ptr<storage::DataTable::SlotIterator> iter_ = nullptr;
  std::unique_ptr<storage::DataTable::SlotIterator> iter_end_ = nullptr;
  bool initialized_ = false;
};

}  // namespace terrier::execution::sql
