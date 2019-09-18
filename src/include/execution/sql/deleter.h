#pragma once
#include "execution/util/execution_common.h"
#include "execution/exec/execution_context.h"

namespace terrier::execution::sql {

/**
 * Helper class to perform deletes in SQL Tables.
 */
class EXPORT Deleter {
 public:
  Deleter(exec::ExecutionContext *exec_ctx, std::string table_name)
  : Deleter(exec_ctx, exec_ctx->GetAccessor()->GetTableOid(table_name)) {}

  Deleter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid)
  : table_oid_(table_oid), exec_ctx_(exec_ctx) {}

  void TableDelete(storage::TupleSlot slot) {
    common::ManagedPointer<storage::SqlTable> sql_table = exec_ctx_->GetAccessor()->GetTable(table_oid_);
    transaction::TransactionContext *txn = exec_ctx_->GetTxn();
    sql_table->Delete(txn, slot);
  }

 private:
  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
};
}  // namespace terrier::execution::sql