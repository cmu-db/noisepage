#pragma once
#include <memory>
#include "execution/exec/output.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace tpl::exec {
using terrier::transaction::TransactionContext;

class ExecutionContext {
 public:
  ExecutionContext(TransactionContext *txn,
                   OutputCallback callback,
                   const std::shared_ptr<FinalSchema> &final_schema)
      : txn_(txn),
        buffer_(std::make_unique<OutputBuffer>(final_schema->GetCols().size(),
                                               ComputeTupleSize(final_schema),
                                               callback)) {}

  TransactionContext *GetTxn() { return txn_; }

  OutputBuffer *GetOutputBuffer() { return buffer_.get(); }

  static uint32_t ComputeTupleSize(
      const std::shared_ptr<FinalSchema> &final_schema) {
    uint32_t tuple_size = 0;
    for (const auto &col : final_schema->GetCols()) {
      tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
    }
    return tuple_size;
  }

 private:
  TransactionContext *txn_;
  std::unique_ptr<OutputBuffer> buffer_;
};
}  // namespace tpl::exec