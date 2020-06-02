
#include <execution/sql/iter_cte_scan_iterator.h>

#include "execution/sql/cte_scan_iterator.h"
#include "execution/sql/iter_cte_scan_iterator.h"

#include "parser/expression/constant_value_expression.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"

namespace terrier::execution::sql {

IterCteScanIterator::IterCteScanIterator(terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t *schema_cols_type,
                                 uint32_t num_schema_cols)
    : cte_scan_1_{exec_ctx, schema_cols_type, num_schema_cols},
    cte_scan_2_{exec_ctx, schema_cols_type, num_schema_cols}, cte_scan_read_{&cte_scan_1_},
    cte_scan_write_{&cte_scan_2_}, written_{false} {}

storage::SqlTable *IterCteScanIterator::GetWriteTable() {
  return cte_scan_write_->GetTable();
}

storage::SqlTable *IterCteScanIterator::GetReadTable() {
  return cte_scan_read_->GetTable();
}

CteScanIterator *IterCteScanIterator::GetResultCTE() {
  return &cte_scan_read_;
}

catalog::table_oid_t IterCteScanIterator::GetReadTableOid() {
  return cte_scan_write_->GetTableOid();
}

storage::ProjectedRow *IterCteScanIterator::GetInsertTempTablePR() {
  return cte_scan_write_->GetInsertTempTablePR();
}

storage::TupleSlot IterCteScanIterator::TableInsert() {
  written_ = true;
  return cte_scan_write_->TableInsert();
}

bool IterCteScanIterator::Accumulate() {

  // swap the tables
  auto temp_table = cte_scan_write_;
  cte_scan_write_ = cte_scan_read_;
  cte_scan_read_ = temp_table;

  if(written_){
    // clear new write table
    cte_scan_write_->GetTable()->Reset();
  }

  bool old_written_ = written_;
  written_ = false;
  return old_written_;
}

}  // namespace terrier::execution::sql
