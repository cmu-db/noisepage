#include "planner/plannodes/create_table_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::planner {
common::hash_t CreateTablePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash namespace_oid
  auto namespace_oid = GetNamespaceOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // TODO(Gus,Wen) Hash catalog::Schema

  // Hash has primary_key
  auto has_primary_key = HasPrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&has_primary_key));

  // Hash primary_key
  hash = common::HashUtil::CombineHashes(hash, primary_key_.Hash());

  // Hash foreign_keys
  for (const auto &foreign_key : foreign_keys_) {
    hash = common::HashUtil::CombineHashes(hash, foreign_key.Hash());
  }

  // Hash con_uniques
  for (const auto &con_unique : con_uniques_) {
    hash = common::HashUtil::CombineHashes(hash, con_unique.Hash());
  }

  // Hash con_checks
  for (const auto &con_check : con_checks_) {
    hash = common::HashUtil::CombineHashes(hash, con_check.Hash());
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateTablePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateTablePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // TODO(Gus,Wen) Compare catalog::Schema

  // Has primary key
  if (HasPrimaryKey() != other.HasPrimaryKey()) return false;

  // Primary Key
  if (HasPrimaryKey() && (GetPrimaryKey() != other.GetPrimaryKey())) return false;

  // Foreign key
  const auto &foreign_keys_ = GetForeignKeys();
  const auto &other_foreign_keys_ = other.GetForeignKeys();
  if (foreign_keys_.size() != other_foreign_keys_.size()) return false;

  for (size_t i = 0; i < foreign_keys_.size(); i++) {
    if (foreign_keys_[i] != other_foreign_keys_[i]) {
      return false;
    }
  }

  // Unique constraints
  const auto &con_uniques = GetUniqueConstraintss();
  const auto &other_con_uniques = other.GetUniqueConstraintss();
  if (con_uniques.size() != other_con_uniques.size()) return false;

  for (size_t i = 0; i < con_uniques.size(); i++) {
    if (con_uniques[i] != other_con_uniques[i]) {
      return false;
    }
  }

  // Check constraints
  const auto &con_checks = GetCheckConstrinats();
  const auto &other_con_check = other.GetCheckConstrinats();
  if (con_checks.size() != other_con_check.size()) return false;

  for (size_t i = 0; i < con_checks.size(); i++) {
    if (con_checks[i] != other_con_check[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json CreateTablePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_name"] = table_name_;
  j["table_schema"] = table_schema_;

  j["has_primary_key"] = has_primary_key_;
  if (has_primary_key_) {
    j["primary_key"] = primary_key_;
  }

  j["foreign_keys"] = foreign_keys_;
  j["con_uniques"] = con_uniques_;
  j["con_checks"] = con_checks_;
  return j;
}

void CreateTablePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_name_ = j.at("table_name").get<std::string>();

  if (!j.at("table_schema").is_null()) {
    table_schema_ = catalog::Schema::DeserializeSchema(j.at("table_schema"));
  }

  has_primary_key_ = j.at("has_primary_key").get<bool>();
  if (has_primary_key_) {
    primary_key_ = j.at("primary_key").get<PrimaryKeyInfo>();
  }

  foreign_keys_ = j.at("foreign_keys").get<std::vector<ForeignKeyInfo>>();
  con_uniques_ = j.at("con_uniques").get<std::vector<UniqueInfo>>();
  con_checks_ = j.at("con_checks").get<std::vector<CheckInfo>>();
}

}  // namespace terrier::planner
