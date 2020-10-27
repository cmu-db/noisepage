#include "planner/plannodes/nested_loop_join_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t NestedLoopJoinPlanNode::Hash() const { return AbstractJoinPlanNode::Hash(); }

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  // There is nothing else for us to do here! Go home! You're drunk!
  // Unfortunately, now there is something to do...
  return AbstractJoinPlanNode::operator==(rhs);
}

nlohmann::json NestedLoopJoinPlanNode::ToJson() const { return AbstractJoinPlanNode::ToJson(); }

std::vector<std::unique_ptr<parser::AbstractExpression>> NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractJoinPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(NestedLoopJoinPlanNode);

}  // namespace noisepage::planner
