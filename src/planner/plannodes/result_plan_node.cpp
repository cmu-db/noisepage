#include "planner/plannodes/result_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::planner {

common::hash_t ResultPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash expr
  hash = common::HashUtil::CombineHashes(hash, expr_->Hash());

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ResultPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const ResultPlanNode &>(rhs);

  // expr
  auto expr = GetExpression();
  auto other_expr = other.GetExpression();
  if ((expr != nullptr && other_expr == nullptr) || (expr == nullptr && other_expr != nullptr)) return false;

  if (expr != nullptr && *expr != *other_expr) return false;

  return AbstractPlanNode::operator==(rhs);
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
