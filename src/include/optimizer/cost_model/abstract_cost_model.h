
#pragma once

#include "optimizer/group_expression.h"
#include "optimizer/operator_visitor.h"
#include "transaction/transaction_context.h"

namespace terrier {
namespace optimizer {

class Memo;

// Default cost when cost model cannot compute correct cost.
static constexpr double DEFAULT_COST = 1;

// Estimate the cost of processing each row during a query.
static constexpr double DEFAULT_TUPLE_COST = 0.01;

// Estimate the cost of processing each index entry during an index scan.
static constexpr double DEFAULT_INDEX_TUPLE_COST = 0.005;

// Estimate the cost of processing each operator or function executed during a
// query.
static constexpr double DEFAULT_OPERATOR_COST = 0.0025;

class AbstractCostModel : public terrier::optimizer::OperatorVisitor {
 public:
  virtual double CalculateCost(GroupExpression *gexpr, Memo *memo, transaction::TransactionContext *txn) = 0;
};

}  // namespace optimizer
}  // namespace terrier
