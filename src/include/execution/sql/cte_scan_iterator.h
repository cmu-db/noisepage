#pragma once

#include <vector>
#include "execution/exec/execution_context.h"
#include "storage/sql_table.h"

namespace noisepage::execution::sql {

/**
 * An iterator over a CTE Temp table's data.
 */
class EXPORT CteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */
  CteScanIterator(execution::exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                  uint32_t *schema_cols_ids, uint32_t *schema_cols_type, uint32_t num_schema_cols);

  /**
   * Destructor
   */
  ~CteScanIterator() = default;

  /**
   * @return Returns the temporary table that the cte has made
   */
  storage::SqlTable *GetTable();

  /**
   * @return Returns the oid of the temporary table that the cte has made
   */
  catalog::table_oid_t GetTableOid();

  /**
   * @return Returns a projected row of the table for insertion
   */
  storage::ProjectedRow *GetInsertTempTablePR();

  /**
   * @return Returns the slot which was inserted in the table using the projected row
   */
  storage::TupleSlot TableInsert();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CteScanIterator);

 private:
  /** The execution context */
  noisepage::execution::exec::ExecutionContext *exec_ctx_;
  /** The underlying temporary table */
  storage::SqlTable *cte_table_;
  /** The OID for the underlying temporary table */
  catalog::table_oid_t cte_table_oid_;
  /** The OIDs for the columns of the underlying table */
  std::vector<catalog::col_oid_t> col_oids_;
  /** The associated REDO record */
  storage::RedoRecord *table_redo_;
};

}  // namespace noisepage::execution::sql
