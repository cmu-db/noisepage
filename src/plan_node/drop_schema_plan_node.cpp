#include "plan_node/drop_schema_plan_node.h"
#include <string>
#include <utility>

namespace terrier::plan_node {
common::hash_t DropSchemaPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetSchemaName()));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&if_exist));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DropSchemaPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropSchemaPlanNode &>(rhs);

  // Schema name
  if (GetSchemaName() != other.GetSchemaName()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
