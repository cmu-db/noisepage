#include "plan_node/create_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"

namespace terrier::plan_node {
common::hash_t CreatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreatePlanNode &>(rhs);

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
