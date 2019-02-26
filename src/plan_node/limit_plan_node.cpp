#include "plan_node/limit_plan_node.h"
#include "common/hash_util.h"
#include "plan_node/hash_plan_node.h"

namespace terrier::plan_node {

inline common::hash_t LimitPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));

  // TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

inline bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }
  auto &other = static_cast<const plan_node::LimitPlanNode &>(rhs);
  return (limit_ == other.limit_ && offset_ == other.offset_ && AbstractPlanNode::operator==(rhs));
}

}  // namespace terrier::plan_node
