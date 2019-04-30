#include "planner/plannodes/aggregate_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

// TODO(Gus,Wen): include hash for schema
common::hash_t AggregatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  if (GetHavingClausePredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetHavingClausePredicate()->Hash());
  }

  for (auto aggregate_term : aggregate_terms_) {
    hash = common::HashUtil::CombineHashes(hash, aggregate_term->Hash());
  }

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

  if (aggregate_terms_.size() != other.GetAggregateTerms().size()) return false;
  for (size_t i = 0; i < aggregate_terms_.size(); i++) {
    auto &left_term = aggregate_terms_[i];
    auto &right_term = other.GetAggregateTerms()[i];
    if ((left_term == nullptr && right_term != nullptr) || (left_term != nullptr && right_term == nullptr))
      return false;
    if (left_term != nullptr && *left_term != *right_term) return false;
  }

  if (GetAggregateStrategyType() != other.GetAggregateStrategyType()) return false;

  return (AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::planner
