#pragma once

#include <memory>
#include <vector>
#include "execution/sql/cte_scan_iterator.h"

namespace terrier::execution::sql {

/**
 * An iterator over a CTE Temp table's data
 */
class EXPORT IndCteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */

  IndCteScanIterator(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *schema_cols_ids,
                     uint32_t *schema_cols_type, uint32_t num_schema_cols, bool is_recursive);

  /**
   * Returns the temporary table that the cte has made
   */
  CteScanIterator *GetWriteCte();

  /**
   * Returns the temporary table that the cte has made
   */
  CteScanIterator *GetReadCte();

  /**
   * Returns the oid of the temporary table that the cte has made
   */
  catalog::table_oid_t GetReadTableOid();

  /**
   * @return Returns the CTE scan that stores the object
   */
  CteScanIterator *GetResultCTE();

  /**
   * Returns a projected row of the table for insertion
   */
  storage::ProjectedRow *GetInsertTempTablePR();

  /**
   * Returns the slot which was inserted in the table using the projected row
   */

  storage::TupleSlot TableInsert();

  /**
   * Accumulates the results of read and write table to get ready for a new
   * iteration of writing to the write table. In this case, the write and read
   * tables are swapped if the write table isn't empty.
   * @return If the swap happened then true is returned, else false is returned.
   */
  bool Accumulate();

  /**
   * Destructor
   */
  ~IndCteScanIterator() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IndCteScanIterator);

 private:
  exec::ExecutionContext *exec_ctx_;
  CteScanIterator cte_scan_1_;
  CteScanIterator cte_scan_2_;
  CteScanIterator cte_scan_3_;
  CteScanIterator *cte_scan_read_;
  CteScanIterator *cte_scan_write_;
  catalog::table_oid_t table_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  bool written_;
  bool is_recursive_;
};

}  // namespace terrier::execution::sql
