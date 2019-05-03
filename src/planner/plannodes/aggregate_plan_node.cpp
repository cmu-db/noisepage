#include "planner/plannodes/aggregate_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

// TODO(Gus,Wen): include hash for schema
common::hash_t AggregatePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  if (GetHavingClausePredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetHavingClausePredicate()->Hash());
  }

  for (auto &aggregate_term : aggregate_terms_) {
    hash = common::HashUtil::CombineHashes(hash, aggregate_term->Hash());
  }

  auto agg_strategy = GetAggregateStrategyType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&agg_strategy));

  return hash;
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

nlohmann::json AggregatePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["having_clause_predicate"] = having_clause_predicate_;
  j["aggregate_terms"] = aggregate_terms_;
  j["aggregate_strategy"] = aggregate_strategy_;
  return j;
}

void AggregatePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  if (!j.at("having_clause_predicate").is_null()) {
    having_clause_predicate_ = parser::DeserializeExpression(j.at("having_clause_predicate"));
  }

  // Deserialize aggregate terms
  auto aggregate_term_jsons = j.at("aggregate_terms").get<std::vector<nlohmann::json>>();
  for (const auto &json : aggregate_term_jsons) {
    aggregate_terms_.push_back(
        std::dynamic_pointer_cast<parser::AggregateExpression>(parser::DeserializeExpression(json)));
  }

  aggregate_strategy_ = j.at("aggregate_strategy").get<AggregateStrategyType>();
}

}  // namespace terrier::planner
