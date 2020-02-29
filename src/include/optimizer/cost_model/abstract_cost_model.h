#pragma once

#include <unordered_map>
#include "optimizer/operator_visitor.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}

namespace optimizer {

class GroupExpression;
class Memo;

/**
 * Storing statistics about a cost model
 * Including cost and cardinalities of all plan nodes
 */
struct CostModelEstimate {
  /**
   * Cost of a plan
   */
  double cost_;
  /**
   * Mapping from plan node id to cost value
   */
  std::unordered_map<plan_node_id_t, double> cardinalities_;

  /**
   * CostModelEstimate Constructor
   * @param cost Cost value
   * @param cardinalities Mapping from plan node id to cost value
   */
  CostModelEstimate(double cost, std::unordered_map<plan_node_id_t, double> cardinalities)
      : cost_(cost), cardinalities_(std::move(cardinalities)) {}
};
/**
 * Interface defining a cost model.
 * A cost model's primary entrypoint is CalculateCost()
 */
class AbstractCostModel : public OperatorVisitor {
 public:
  /**
   * Costs a GroupExpression
   * @param txn TransactionContext that query is generated under
   * @param memo Memo object containing all relevant groups
   * @param gexpr GroupExpression to calculate cost for
   */
  virtual CostModelEstimate CalculateCost(transaction::TransactionContext *txn, Memo *memo, GroupExpression *gexpr) = 0;
};

}  // namespace optimizer
}  // namespace terrier
