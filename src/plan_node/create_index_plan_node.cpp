#include "plan_node/create_index_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::plan_node {
common::hash_t CreateIndexPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash index_type
  auto index_type = GetIndexType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&index_type));

  // Hash index_attrs
  hash = common::HashUtil::CombineHashInRange(hash, index_attrs_.begin(), index_attrs_.end());

  // Hash key_attrs
  hash = common::HashUtil::CombineHashInRange(hash, key_attrs_.begin(), key_attrs_.end());

  // Hash unique_index
  auto unique_index = IsUniqueIndex();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&unique_index));

  // Hash index_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetIndexName()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateIndexPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Index type
  if (GetIndexType() != other.GetIndexType()) return false;

  // Index attrs
  const auto &index_attrs = GetIndexAttributes();
  const auto &other_index_attrs = other.GetIndexAttributes();
  if (index_attrs.size() != other_index_attrs.size()) return false;

  for (size_t i = 0; i < index_attrs.size(); i++) {
    if (index_attrs[i] != other_index_attrs[i]) {
      return false;
    }
  }

  // Key attrs
  const auto &key_attrs = GetKeyAttrs();
  const auto &other_key_attrs = other.GetKeyAttrs();
  if (key_attrs.size() != other_key_attrs.size()) return false;

  for (size_t i = 0; i < key_attrs.size(); i++) {
    if (key_attrs[i] != other_key_attrs[i]) {
      return false;
    }
  }

  // Unique index
  if (IsUniqueIndex() != other.IsUniqueIndex()) return false;

  // Index name
  if (GetIndexName() != other.GetIndexName()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
