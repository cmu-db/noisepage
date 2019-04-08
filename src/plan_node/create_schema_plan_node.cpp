#include "plan_node/create_schema_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::plan_node {
common::hash_t CreateSchemaPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetSchemaName()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateSchemaPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateSchemaPlanNode &>(rhs);

  // Schema name
  if (GetSchemaName() != other.GetSchemaName()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
