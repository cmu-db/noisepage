#include "plan_node/populate_index_plan_node.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::plan_node {
common::hash_t PopulateIndexPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash target_table_oid
  auto target_table_oid = GetTargetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&target_table_oid));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash column_names
  for (const auto column_oid : column_oids_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&column_oid));
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool PopulateIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const PopulateIndexPlanNode &>(rhs);

  // Target table OID
  if (GetTargetTableOid() != other.GetTargetTableOid()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Column names
  const auto &column_oids = GetColumnOids();
  const auto &other_column_oids = other.GetColumnOids();
  if (column_oids.size() != other_column_oids.size()) return false;

  for (size_t i = 0; i < column_oids.size(); i++) {
    if (column_oids[i] != other_column_oids[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
