#include "plan_node/aggregate_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::plan_node {

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

  auto agg_strategy = GetAggregateStrategy();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&agg_strategy));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool AggregatePlanNode::AreEqual(const std::vector<AggregatePlanNode::AggregateTerm> &A,
                                 const std::vector<AggregatePlanNode::AggregateTerm> &B) const {
  if (A.size() != B.size()) return false;

  for (size_t i = 0; i < A.size(); i++) {
    if (A[i].aggregate_type_ != B[i].aggregate_type_) return false;

    auto *expr = A[i].expression_;

    if (expr != nullptr && (*expr != *(B[i].expression_))) return false;

    if (A[i].distinct_ != B[i].distinct_) return false;
  }
  return true;
}

bool AggregatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const AggregatePlanNode &>(rhs);

  auto *pred = GetHavingClausePredicate();
  auto *other_pred = other.GetHavingClausePredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) return false;
  if (pred != nullptr && *pred != *other_pred) return false;

  if (!AreEqual(GetAggregateTerms(), other.GetAggregateTerms())) return false;

  if (GetAggregateStrategy() != other.GetAggregateStrategy()) return false;

  return (AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::plan_node
