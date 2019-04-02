#include "plan_node/analyze_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"

namespace terrier::plan_node {

common::hash_t AnalyzePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash column_names
  for (const auto &column_name : column_names_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_name));
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool AnalyzePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const AnalyzePlanNode &>(rhs);

  // Target table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Column names
  const auto &column_names = GetColumnNames();
  const auto &other_column_names = other.GetColumnNames();
  if (column_names.size() != other_column_names.size()) return false;

  for (size_t i = 0; i < column_names.size(); i++) {
    if (column_names[i] != other_column_names[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
