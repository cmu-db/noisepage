#pragma once

#include <memory>
#include <vector>
#include "execution/exec/execution_context.h"
#include "execution/sql/table_vector_iterator.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {

/**
 * An iterator over a CTE Temp table's data
 */
class EXPORT CteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */

  CteScanIterator(terrier::execution::exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                  uint32_t *schema_cols_ids, uint32_t *schema_cols_type, uint32_t num_schema_cols);

  /**
   * Returns the temporary table that the cte has made
   */
  storage::SqlTable *GetTable();

  /**
   * Returns the oid of the temporary table that the cte has made
   */
  catalog::table_oid_t GetTableOid();

  /**
   * Returns a projected row of the table for insertion
   */
  storage::ProjectedRow *GetInsertTempTablePR();

  /**
   * Returns the slot which was inserted in the table using the projected row
   */

  storage::TupleSlot TableInsert();

  /**
   * Return the iterator that will scan the temp table
   */

  /**
   * Destructor
   */
  ~CteScanIterator() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CteScanIterator);

 private:
  terrier::execution::exec::ExecutionContext *exec_ctx_;
  storage::SqlTable *cte_table_;
  catalog::table_oid_t cte_table_oid_;
  std::vector<catalog::col_oid_t> col_oids_;
  storage::RedoRecord *table_redo_;
};

}  // namespace terrier::execution::sql
