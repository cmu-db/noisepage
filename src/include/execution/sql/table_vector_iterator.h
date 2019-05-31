#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/sql_table.h"

namespace tpl::sql {
using terrier::catalog::db_oid_t;
using terrier::catalog::table_oid_t;
using terrier::storage::DataTable;
using terrier::transaction::TransactionContext;

class ThreadStateContainer;

/**
 * An iterator over a table's data in vector-wise fashion
 */
class TableVectorIterator {
 public:
  /**
   * Minimum block range
   */
  static constexpr const u32 kMinBlockRangeSize = 2;

  /**
   * Create a new vectorized iterator over the given table
   * @param db_oid database oid of the table
   * @param table_oid oid of the table
   * @param txn transaction to use when scanning the table
   */
  explicit TableVectorIterator(u32 db_oid, u32 table_oid, TransactionContext *txn);

  /**
   * Destructor
   */
  ~TableVectorIterator();

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
   * Scan function callback used to scan a partition of the table
   */
  using ScanFn = void (*)(exec::ExecutionContext *, void *, TableVectorIterator *iter);

  /**
   * Perform a parallel scan over the table with ID @em table_oid using the
   * callback function @em scanner on each input vector projection from the
   * source table. This call is blocking, meaning that it only returns after
   * the whole table has been scanned. Iteration order is non-deterministic.
   * @param db_oid The ID of the database containing the table
   * @param table_oid The ID of the table
   * @param exec_ctx The runtime context passed into the callback function
   * @param thread_state_container The thread state container
   * @param scanner The callback function invoked for vectors of table input
   * @param min_grain_size The minimum number of blocks to give a scan task
   */
  static bool ParallelScan(u32 db_oid, u32 table_oid, exec::ExecutionContext *exec_ctx,
                           ThreadStateContainer *thread_state_container, ScanFn scanner,
                           u32 min_grain_size = kMinBlockRangeSize);

 private:
  const db_oid_t db_oid_;
  const table_oid_t table_oid_;
  TransactionContext* txn_;

  // TODO(Amadou): These are temporary variables until transactions are in
  // And until the Init() logic is in the bytecode emitter.
  bool null_txn_;
  bool initialized = false;

  // SqlTable to iterate over
  std::shared_ptr<terrier::catalog::SqlTableRW> catalog_table_;

  // The PCI
  ProjectedColumnsIterator pci_;

  // A PC and its buffer of the PC.
  byte *buffer_ = nullptr;
  terrier::storage::ProjectedColumns *projected_columns_ = nullptr;
  // Iterator of the slots in the PC
  std::unique_ptr<DataTable::SlotIterator> iter_ = nullptr;
};

}  // namespace tpl::sql
