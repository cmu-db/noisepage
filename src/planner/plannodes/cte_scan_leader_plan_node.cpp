#include "planner/plannodes/cte_scan_leader_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t CteScanLeaderPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();
  return hash;
}

bool CteScanLeaderPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;
  return true;
}

nlohmann::json CteScanLeaderPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CteScanLeaderPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  return exprs;
}

}  // namespace terrier::planner
