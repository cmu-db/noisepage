#include "plan_node/aggregate_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> AggregatePlanNode::Copy() const {
  std::vector<AggregateTerm> copied_aggregate_terms;
  for (const AggregateTerm &term : GetAggregateTerms()) {
    copied_aggregate_terms.push_back(term.Copy());
  }

  AggregatePlanNode *new_plan = new AggregatePlanNode(
      GetOutputSchema(), std::unique_ptr<const parser::AbstractExpression>(having_clause_predicate_->Copy()),
      std::move(copied_aggregate_terms), GetAggregateStrategy());
  return std::unique_ptr<AbstractPlanNode>(new_plan);
}

common::hash_t AggregatePlanNode::HashAggregateTerms(
    const std::vector<AggregatePlanNode::AggregateTerm> &aggregate_terms) const {
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

  // TODO(Gus, Wen): Hash output_schema as well

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

    if (expr && (*expr != *(B[i].expression_))) return false;

    if (A[i].distinct_ != B[i].distinct_) return false;
  }
  return true;
}

// TODO(Gus,Wen): include == for schema
bool AggregatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const AggregatePlanNode &>(rhs);

  auto *pred = GetHavingClausePredicate();
  auto *other_pred = other.GetHavingClausePredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) return false;
  if (pred && *pred != *other_pred) return false;

  if (!AreEqual(GetAggregateTerms(), other.GetAggregateTerms())) return false;

  if (GetAggregateStrategy() != other.GetAggregateStrategy()) return false;

  return (AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::plan_node
