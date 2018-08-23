#pragma once

#include <list>
#include <queue>
namespace terrier::transaction {
class TransactionContext;
using TransactionQueue = std::queue<transaction::TransactionContext *, std::list<transaction::TransactionContext *>>;
}  // namespace terrier::transaction
