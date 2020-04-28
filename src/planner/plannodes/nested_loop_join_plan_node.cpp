#include <iterator>
#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "nlohmann/json.hpp"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace terrier::planner {
class AbstractPlanNode;
}  // namespace terrier::planner

namespace terrier::planner {

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

}  // namespace terrier::planner
