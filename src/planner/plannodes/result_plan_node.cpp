#include "planner/plannodes/result_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t ResultPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Expression
  hash = common::HashUtil::CombineHashes(hash, expr_->Hash());

  return hash;
}

bool ResultPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const ResultPlanNode &>(rhs);

  // Expression
  if ((expr_ != nullptr && other.expr_ == nullptr) || (expr_ == nullptr && other.expr_ != nullptr)) return false;
  if (expr_ != nullptr && *expr_ != *other.expr_) return false;

  return true;
}

nlohmann::json ResultPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["expr"] = expr_ == nullptr ? nlohmann::json(nullptr) : expr_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> ResultPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  if (!j.at("expr").is_null()) {
    auto deserialized = parser::DeserializeExpression(j.at("expr"));
    expr_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ResultPlanNode);

}  // namespace noisepage::planner
