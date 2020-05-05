#include "planner/plannodes/cte_scan_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t CteScanPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();
  return hash;
}

bool CteScanPlanNode::operator==(const AbstractPlanNode &rhs) const { return AbstractPlanNode::operator==(rhs); }

nlohmann::json CteScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CteScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  return exprs;
}

}  // namespace terrier::planner
