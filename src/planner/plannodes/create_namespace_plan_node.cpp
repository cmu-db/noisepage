#include "planner/plannodes/create_namespace_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::planner {
common::hash_t CreateNamespacePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetNamespaceName()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateNamespacePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Schema name
  if (GetNamespaceName() != other.GetNamespaceName()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::planner
