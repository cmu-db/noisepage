#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/sql_table.h"

namespace terrier::sql {
using terrier::catalog::db_oid_t;
using terrier::catalog::namespace_oid_t;
using terrier::catalog::table_oid_t;
using terrier::storage::DataTable;

class ThreadStateContainer;

/**
 * An iterator over a table's data in vector-wise fashion.
 * TODO(Amadou): Add a Reset() method to avoid reconstructing the object in NL joins.
 */
class TableVectorIterator {
 public:
  /**
   * Minimum block range
   */
  static constexpr const u32 kMinBlockRangeSize = 2;

  /**
   * Create a new vectorized iterator over the given table
   * @param table_oid oid of the table
   * @param exec_ctx execution context of the query
   */
  explicit TableVectorIterator(u32 table_oid, exec::ExecutionContext *exec_ctx);

  /**
   * Destructor
   */
  ~TableVectorIterator();

  /**
   * Add a column to the list of columns to scan
   * @param col_oid oid of the column to scan
   */
  void AddCol(u32 col_oid) { col_oids_.emplace_back(col_oid); }

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(TableVectorIterator);

  /**
   * Advance the iterator by a vector of input
   * @return True if there is more data in the iterator; false otherwise
   */
  bool Advance();

  /**
   * Initialize the iterator, returning true if the initialization succeeded
   * @return True if the initialization succeeded; false otherwise
   */
  bool Init();

  /**
   * @return the iterator over the current active projection
   */
  ProjectedColumnsIterator *projected_columns_iterator() { return &pci_; }

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
  static bool ParallelScan(u32 db_oid, u32 table_oid, void *query_state, ThreadStateContainer *thread_states,
                           ScanFn scan_fn, u32 min_grain_size = kMinBlockRangeSize);

 private:
  // The PCI
  ProjectedColumnsIterator pci_;
  const table_oid_t table_oid_;
  // SqlTable to iterate over
  terrier::common::ManagedPointer<terrier::storage::SqlTable> table_{nullptr};
  std::vector<terrier::catalog::col_oid_t> col_oids_{};
  // A PC and its buffer.
  byte *buffer_ = nullptr;
  terrier::storage::ProjectedColumns *projected_columns_ = nullptr;

  // Iterator of the slots in the PC
  std::unique_ptr<DataTable::SlotIterator> iter_ = nullptr;
  exec::ExecutionContext *exec_ctx_;

  bool initialized = false;
};

}  // namespace terrier::sql
