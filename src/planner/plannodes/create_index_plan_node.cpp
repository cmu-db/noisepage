#include "planner/plannodes/create_index_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t CreateIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash index_type
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_type_));

  // Hash index_attrs
  hash = common::HashUtil::CombineHashInRange(hash, index_attrs_.begin(), index_attrs_.end());

  // Hash key_attrs
  hash = common::HashUtil::CombineHashInRange(hash, key_attrs_.begin(), key_attrs_.end());

  // Hash unique_index
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(unique_index_));

  // Hash index_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_name_));

  return hash;
}

bool CreateIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateIndexPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Table OID
  if (table_oid_ != other.table_oid_) return false;

  // Index type
  if (index_type_ != other.index_type_) return false;

  // Index attrs
  if (index_attrs_ != other.index_attrs_) return false;

  // Key attrs
  if (key_attrs_ != other.key_attrs_) return false;

  // Unique index
  if (unique_index_ != other.unique_index_) return false;

  // Index name
  if (index_name_ != other.index_name_) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json CreateIndexPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["index_type"] = index_type_;
  j["unique_index"] = unique_index_;
  j["index_name"] = index_name_;
  j["index_attrs"] = index_attrs_;
  j["key_attrs"] = key_attrs_;
  return j;
}

void CreateIndexPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  index_type_ = j.at("index_type").get<parser::IndexType>();
  unique_index_ = j.at("unique_index").get<bool>();
  index_name_ = j.at("index_name").get<std::string>();
  index_attrs_ = j.at("index_attrs").get<std::vector<std::string>>();
  key_attrs_ = j.at("key_attrs").get<std::vector<std::string>>();
}

}  // namespace terrier::planner
