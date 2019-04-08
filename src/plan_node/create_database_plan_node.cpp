#include "plan_node/create_database_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::plan_node {
common::hash_t CreateDatabasePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetDatabaseName()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateDatabasePlanNode &>(rhs);

  // Database name
  if (GetDatabaseName() != other.GetDatabaseName()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
