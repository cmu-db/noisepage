#include "planner/plannodes/aggregate_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

common::hash_t AggregatePlanNode::HashAggregateTerms(
    const std::vector<AggregatePlanNode::AggregateTerm> &agg_terms) const {
  common::hash_t hash = 0;

  for (auto &agg_term : GetAggregateTerms()) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&agg_term.aggregate_type_));

    if (agg_term.expression_ != nullptr) hash = common::HashUtil::CombineHashes(hash, agg_term.expression_->Hash());

    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&agg_term.distinct_));
  }
  return hash;
}

// TODO(Gus,Wen): include hash for schema
common::hash_t AggregatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  if (GetHavingClausePredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetHavingClausePredicate()->Hash());
  }

  hash = common::HashUtil::CombineHashes(hash, HashAggregateTerms(GetAggregateTerms()));

  auto agg_strategy = GetAggregateStrategyType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&agg_strategy));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool AggregatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const AggregatePlanNode &>(rhs);

  auto &pred = GetHavingClausePredicate();
  auto &other_pred = other.GetHavingClausePredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) return false;
  if (pred != nullptr && *pred != *other_pred) return false;

  if (GetAggregateTerms() != other.GetAggregateTerms()) return false;

  if (GetAggregateStrategyType() != other.GetAggregateStrategyType()) return false;

  return (AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::planner
