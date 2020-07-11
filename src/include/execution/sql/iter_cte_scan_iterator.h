#pragma once

#include <memory>
#include <vector>
#include "execution/sql/cte_scan_iterator.h"

namespace terrier::execution::sql {

/**
 * An iterator over a CTE Temp table's data
 */
class EXPORT IterCteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */

  IterCteScanIterator(terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t *schema_cols_type,
                      uint32_t num_schema_cols);

  /**
   * Returns the temporary table that the cte has made
   */
  storage::SqlTable *GetWriteTable();

  /**
   * Returns the temporary table that the cte has made
   */
  storage::SqlTable *GetReadTable();

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
  ~IterCteScanIterator() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IterCteScanIterator);

 private:
  CteScanIterator cte_scan_1_;
  CteScanIterator cte_scan_2_;
  CteScanIterator *cte_scan_read_;
  CteScanIterator *cte_scan_write_;
  bool written_;
};

}  // namespace terrier::execution::sql