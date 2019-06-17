#include "planner/plannodes/aggregate_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

common::hash_t AggregatePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Having Clause Predicate
  if (having_clause_predicate_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, having_clause_predicate_->Hash());
  }

  // Aggregtation Terms
  for (auto &aggregate_term : aggregate_terms_) {
    hash = common::HashUtil::CombineHashes(hash, aggregate_term->Hash());
  }

  // Aggregate Strategy
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(aggregate_strategy_));

  return hash;
}

bool AggregatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const AggregatePlanNode &>(rhs);

  // Having Clause Predicate
  if ((having_clause_predicate_ == nullptr && other.having_clause_predicate_ != nullptr) ||
      (having_clause_predicate_ != nullptr && other.having_clause_predicate_ == nullptr))
    return false;
  if (having_clause_predicate_ != nullptr && *having_clause_predicate_ != *having_clause_predicate_) return false;

  // Aggregation Terms
  if (aggregate_terms_.size() != other.GetAggregateTerms().size()) return false;
  for (size_t i = 0; i < aggregate_terms_.size(); i++) {
    auto &left_term = aggregate_terms_[i];
    auto &right_term = other.aggregate_terms_[i];
    if ((left_term == nullptr && right_term != nullptr) || (left_term != nullptr && right_term == nullptr))
      return false;
    if (left_term != nullptr && *left_term != *right_term) return false;
  }

  // Aggregate Strategy
  if (aggregate_strategy_ != other.aggregate_strategy_) return false;

  return true;
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
