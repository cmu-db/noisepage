#include "planner/plannodes/aggregate_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t AggregatePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Group By Terms
  for (auto &groupby_term : groupby_terms_) {
    hash = common::HashUtil::CombineHashes(hash, groupby_term->Hash());
  }

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

  // Group By Terms
  if (groupby_terms_.size() != other.GetGroupByTerms().size()) return false;
  for (size_t i = 0; i < groupby_terms_.size(); i++) {
    auto &left_term = groupby_terms_[i];
    auto &right_term = other.groupby_terms_[i];
    if ((left_term == nullptr && right_term != nullptr) || (left_term != nullptr && right_term == nullptr))
      return false;
    if (left_term != nullptr && *left_term != *right_term) return false;
  }

  // Having Clause Predicate
  if ((having_clause_predicate_ == nullptr && other.having_clause_predicate_ != nullptr) ||
      (having_clause_predicate_ != nullptr && other.having_clause_predicate_ == nullptr))
    return false;
  if (having_clause_predicate_ != nullptr && *having_clause_predicate_ != *other.having_clause_predicate_) return false;

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
  return aggregate_strategy_ == other.aggregate_strategy_;
}

nlohmann::json AggregatePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  if (having_clause_predicate_) {
    j["having_clause_predicate"] = having_clause_predicate_->ToJson();
  }

  j["groupby_terms"] = groupby_terms_;
  j["aggregate_terms"] = aggregate_terms_;
  j["aggregate_strategy"] = aggregate_strategy_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AggregatePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  if (!j.at("having_clause_predicate").is_null()) {
    auto deserialized = parser::DeserializeExpression(j.at("having_clause_predicate"));
    having_clause_predicate_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  // Deserialize GroupBy Terms
  auto groupby_term_jsons = j.at("groupby_terms").get<std::vector<nlohmann::json>>();
  for (const auto &json : groupby_term_jsons) {
    auto deserialized = parser::DeserializeExpression(json);
    auto gb_ptr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<parser::AbstractExpression>();
    groupby_terms_.emplace_back(gb_ptr);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  // Deserialize aggregate terms
  auto aggregate_term_jsons = j.at("aggregate_terms").get<std::vector<nlohmann::json>>();
  for (const auto &json : aggregate_term_jsons) {
    auto deserialized = parser::DeserializeExpression(json);
    auto agg_ptr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<parser::AggregateExpression>();
    aggregate_terms_.emplace_back(agg_ptr);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  aggregate_strategy_ = j.at("aggregate_strategy").get<AggregateStrategyType>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(AggregatePlanNode);

}  // namespace noisepage::planner
