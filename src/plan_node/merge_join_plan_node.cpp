#include "plan_node/merge_join_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> MergeJoinPlanNode::Copy() const {
  // TODO(Gus,Wen) The base class AbstractExpression (predicate) does not have a copy function
  // Need to implement copy mechanism
  std::unique_ptr<AbstractPlanNode> dummy;
  return dummy;
}

}  // namespace terrier::plan_node