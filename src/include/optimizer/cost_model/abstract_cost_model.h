#pragma once

#include "optimizer/operator_visitor.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}

namespace optimizer {

class GroupExpression;
class Memo;

/**
 * Interface defining a cost model.
 * A cost model's primary entrypoint is CalculateCost()
 */
class AbstractCostModel : public OperatorVisitor {
 public:
  /**
   * Costs a GroupExpression
   * @param txn TransactionContext that query is generated under
   * @param gexpr GroupExpression to calculate cost for
   * @param memo Memo object containing all relevant groups
   */
  virtual double CalculateCost(transaction::TransactionContext *txn, GroupExpression *gexpr, Memo *memo) = 0;
};

}  // namespace optimizer
}  // namespace terrier
