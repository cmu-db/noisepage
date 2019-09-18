#pragma once
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * Helper class to perform updates in SQL Tables.
 */
class EXPORT Updater {
 public:
  Updater(exec::ExecutionContext *exec_ctx, std::string table_name, std::vector<catalog::col_oid_t> col_oids)
    : Updater(exec_ctx, exec_ctx->GetAccessor()->GetTableOid(table_name), col_oids) {}

  Updater(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> col_oids)
  : table_oid_(table_oid), col_oids_(col_oids), exec_ctx_(exec_ctx) {
    common::ManagedPointer<storage::SqlTable> sql_table = exec_ctx_->GetAccessor()->GetTable(table_oid_);
    transaction::TransactionContext *txn = exec_ctx_->GetTxn();
    storage::ProjectedRowInitializer initializer = sql_table->InitializerForProjectedRow(col_oids_);

    // TODO: If you StageWrite anything that you didn't succeed in writing into the table or decide you don't
    //  want to use, the transaction MUST abort
    redo_record_ = txn->StageWrite(exec_ctx_->DBOid(), table_oid_, initializer);
  }

  storage::ProjectedRow *GetTablePR() {
    return redo_record_->Delta();
  }

  void TableUpdate() {
    common::ManagedPointer<storage::SqlTable> sql_table = exec_ctx_->GetAccessor()->GetTable(table_oid_);
    transaction::TransactionContext *txn = exec_ctx_->GetTxn();
    sql_table->Update(txn, redo_record_);
  }

 private:
  catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> col_oids_;
  exec::ExecutionContext *exec_ctx_;
  storage::RedoRecord *redo_record_;
};
}  // namespace terrier::execution::sql