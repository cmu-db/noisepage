#include "optimizer/cost_model/trivial_cost_model.h"

#include "optimizer/group_expression.h"
#include "optimizer/physical_operators.h"

namespace noisepage::optimizer {

double TrivialCostModel::CalculateCost(transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor,
                                       Memo *memo, GroupExpression *gexpr) {
  gexpr_ = gexpr;
  memo_ = memo;
  txn_ = txn;
  accessor_ = accessor;
  gexpr_->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
  return output_cost_;
}

void TrivialCostModel::Visit(const IndexScan *op) {
  // Get the table schema
  // This heuristic is not really good --- it merely picks the index based on
  // how many of those index's keys are set (op->GetBounds())
  output_cost_ = SCAN_COST - op->GetBounds().size();
}

void TrivialCostModel::Visit(const InnerIndexJoin *op) {
  // Get the table schema
  // This heuristic is not really good --- it merely picks the index based on
  // how many of those index's keys are set (op->GetBounds())
  output_cost_ = NLJOIN_COST - op->GetJoinKeys().size();
}

}  // namespace noisepage::optimizer
