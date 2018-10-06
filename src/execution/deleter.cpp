#include "execution/deleter.h"

#include "common/macros.h"
#include "execution/execution_context.h"
#include "execution/transaction_runtime.h"
#include "loggers/execution_logger.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::execution {

Deleter::Deleter(storage::SqlTable *const table, execution::ExecutionContext *const executor_context)
    : table_(table), executor_context_(executor_context) {
  TERRIER_ASSERT(table != nullptr && executor_context != nullptr, "SqlTable and ExecutionConext must be non-null!");
}

void Deleter::Init(Deleter &deleter, storage::SqlTable *const table, execution::ExecutionContext *const executor_context) {
  new (&deleter) Deleter(table, executor_context);
}

void Deleter::Delete(const storage::TupleSlot slot) {
  EXECUTION_LOG_TRACE("Deleting tuple {}, {} from SqlTable oid {} ", slot.GetBlock(), slot.GetOffset(),
                      table_->GetOid());

  auto *const txn = executor_context_->GetTransaction();

  if (!table_->Delete(txn, slot)) {
    txn->SetTransactionResult(ResultType::FAILURE);
    return;
  }
}

}  // namespace terrier::execution
