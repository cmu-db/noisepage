#include "planner/plannodes/result_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::planner {

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
  j["expr"] = expr_;
  return j;
}

void ResultPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  if (!j.at("expr").is_null()) {
    expr_ = parser::DeserializeExpression(j.at("expr"));
  }
}

}  // namespace terrier::planner
