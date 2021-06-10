#pragma once
#include <memory>
#include <vector>
#include "execution/sql/cte_scan_iterator.h"

namespace noisepage::execution::sql {

/**
 * An iterator over a CTE Temp table's data.
 */
class EXPORT IndCteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */
  IndCteScanIterator(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, uint32_t *schema_cols_ids,
                     uint32_t *schema_cols_type, uint32_t num_schema_cols, bool is_recursive);

  /**
   * Destructor
   */
  ~IndCteScanIterator() = default;

  /**
   * @return Returns the temporary table that the cte has made
   */
  CteScanIterator *GetWriteCte();

  /**
   * @return Returns the temporary table that the cte has made
   */
  CteScanIterator *GetReadCte();

  /**
   * @return Returns the oid of the temporary table that the cte has made
   */
  catalog::table_oid_t GetReadTableOid();

  /**
   * @return Returns the CTE scan that stores the object
   */
  CteScanIterator *GetResultCTE();

  /**
   * @return Returns a projected row of the table for insertion
   */
  storage::ProjectedRow *GetInsertTempTablePR();

  /**
   * @return Returns the slot which was inserted in the table using the projected row
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
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IndCteScanIterator);

 private:
  /** The execution context */
  exec::ExecutionContext *exec_ctx_;

  /**
   * Three CTEScanIterators that we use to store various results;
   * cte_scan_read and cte_scan_write are always pointers to one of these each,
   * though which iterator they point to varies over the lifetime of the operator
   */
  CteScanIterator cte_scan_1_;
  CteScanIterator cte_scan_2_;
  CteScanIterator cte_scan_3_;

  /** Read table containing the results of the inductive queries so far */
  CteScanIterator *cte_scan_read_;
  /** Write table containing the results of the next inductive iteration; these results are then moved into the read
   * table */
  CteScanIterator *cte_scan_write_;

  /** The OID for the underlying temporary table */
  catalog::table_oid_t table_oid_;

  /** The associated transaction */
  common::ManagedPointer<transaction::TransactionContext> txn_;

  /** Flag used internally to track state */
  bool written_;

  /** `true` if the CTE is syntactically recursive */
  bool is_recursive_;
};

}  // namespace noisepage::execution::sql
