#pragma once

#include <memory>
#include <vector>
#include "storage/sql_table.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::sql {

class CteScanIterator {
 public:
  /**
   * Constructor for the CTEScanIterator
   */
  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

  CteScanIterator(terrier::execution::exec::ExecutionContext *exec_ctx,
                  uint32_t *schema_cols_type, uint32_t num_schema_cols) : cte_table_oid_(static_cast<catalog::table_oid_t>(999))
  {
    // Create column metadata for every column.
    std::vector<catalog::Schema::Column> all_columns;
    for (uint32_t i = 0; i < num_schema_cols; i++) {
      catalog::Schema::Column col("col" + std::to_string(i+1),
                                  static_cast<type::TypeId>(schema_cols_type[i]), false,
                                  DummyCVE(), static_cast<catalog::col_oid_t>(i+1));
      all_columns.push_back(col);
    }

    // Create the table in the catalog.
    catalog::Schema cte_table_schema(all_columns);
    cte_table_ = new storage::SqlTable(exec_ctx->GetAccessor()->GetBlockStore(), cte_table_schema);
  }

  /**
   * Returns the temporary table that the cte has made
   */
  storage::SqlTable* GetTable() {
    return cte_table_;
  }

  /**
   * Destructor
   */
  ~CteScanIterator();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CteScanIterator);


  /**
   * Initialize the iterator, returning true if the initialization succeeded
   * @return True if the initialization succeeded; false otherwise
   */
  storage::TupleSlot Next(storage::TupleSlot input) {
    return input;
  }


 private:
  storage::SqlTable* cte_table_;
  catalog::table_oid_t cte_table_oid_;
};

}  // namespace terrier::execution::sql
