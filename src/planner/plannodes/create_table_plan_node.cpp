#include "planner/plannodes/create_table_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<CreateTablePlanNode> CreateTablePlanNode::Builder::Build() {
  return std::unique_ptr<CreateTablePlanNode>(new CreateTablePlanNode(
      std::move(children_), std::move(output_schema_), namespace_oid_, std::move(table_name_), std::move(table_schema_),
      block_store_, has_primary_key_, std::move(primary_key_), std::move(foreign_keys_), std::move(con_uniques_),
      std::move(con_checks_), plan_node_id_));
}

CreateTablePlanNode::CreateTablePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                         std::unique_ptr<OutputSchema> output_schema,
                                         catalog::namespace_oid_t namespace_oid, std::string table_name,
                                         std::unique_ptr<catalog::Schema> table_schema,
                                         common::ManagedPointer<storage::BlockStore> block_store, bool has_primary_key,
                                         PrimaryKeyInfo primary_key, std::vector<ForeignKeyInfo> &&foreign_keys,
                                         std::vector<UniqueInfo> &&con_uniques, std::vector<CheckInfo> &&con_checks,
                                         plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id),
      namespace_oid_(namespace_oid),
      table_name_(std::move(table_name)),
      table_schema_(std::move(table_schema)),
      block_store_(block_store),
      has_primary_key_(has_primary_key),
      primary_key_(std::move(primary_key)),
      foreign_keys_(std::move(foreign_keys)),
      con_uniques_(std::move(con_uniques)),
      con_checks_(std::move(con_checks)) {}

nlohmann::json PrimaryKeyInfo::ToJson() const {
  nlohmann::json j;
  j["primary_key_cols"] = primary_key_cols_;
  j["constraint_name"] = constraint_name_;
  return j;
}

void PrimaryKeyInfo::FromJson(const nlohmann::json &j) {
  primary_key_cols_ = j.at("primary_key_cols").get<std::vector<std::string>>();
  constraint_name_ = j.at("constraint_name").get<std::string>();
}

nlohmann::json ForeignKeyInfo::ToJson() const {
  nlohmann::json j;
  j["foreign_key_sources"] = foreign_key_sources_;
  j["foreign_key_sinks"] = foreign_key_sinks_;
  j["sink_table_name"] = sink_table_name_;
  j["constraint_name"] = constraint_name_;
  j["upd_action"] = upd_action_;
  j["del_action"] = del_action_;
  return j;
}

void ForeignKeyInfo::FromJson(const nlohmann::json &j) {
  foreign_key_sources_ = j.at("foreign_key_sources").get<std::vector<std::string>>();
  foreign_key_sinks_ = j.at("foreign_key_sinks").get<std::vector<std::string>>();
  sink_table_name_ = j.at("sink_table_name").get<std::string>();
  constraint_name_ = j.at("constraint_name").get<std::string>();
  upd_action_ = j.at("upd_action").get<parser::FKConstrActionType>();
  del_action_ = j.at("del_action").get<parser::FKConstrActionType>();
}

nlohmann::json UniqueInfo::ToJson() const {
  nlohmann::json j;
  j["unique_cols"] = unique_cols_;
  j["constraint_name"] = constraint_name_;
  return j;
}

void UniqueInfo::FromJson(const nlohmann::json &j) {
  unique_cols_ = j.at("unique_cols").get<std::vector<std::string>>();
  constraint_name_ = j.at("constraint_name").get<std::string>();
}

nlohmann::json CheckInfo::ToJson() const {
  nlohmann::json j;
  j["check_cols"] = check_cols_;
  j["constraint_name"] = constraint_name_;
  j["expr_type"] = expr_type_;
  j["expr_value"] = expr_value_;
  return j;
}

void CheckInfo::FromJson(const nlohmann::json &j) {
  check_cols_ = j.at("check_cols").get<std::vector<std::string>>();
  constraint_name_ = j.at("constraint_name").get<std::string>();
  expr_type_ = j.at("expr_type").get<parser::ExpressionType>();
  expr_value_ = j.at("expr_value").get<parser::ConstantValueExpression>();
}

common::hash_t CreateTablePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Namespace OI
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Table Name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));

  // Schema
  if (table_schema_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, table_schema_->Hash());
  }

  // Primary Key Flag
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(has_primary_key_));

  // Primary Key Info
  if (has_primary_key_) {
    hash = common::HashUtil::CombineHashes(hash, primary_key_.Hash());
  }

  // Foreign Keys
  for (const auto &foreign_key : foreign_keys_) {
    hash = common::HashUtil::CombineHashes(hash, foreign_key.Hash());
  }

  // Unique Constraints
  for (const auto &con_unique : con_uniques_) {
    hash = common::HashUtil::CombineHashes(hash, con_unique.Hash());
  }

  // Check Constraints
  for (const auto &con_check : con_checks_) {
    hash = common::HashUtil::CombineHashes(hash, con_check.Hash());
  }

  return hash;
}

bool CreateTablePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateTablePlanNode &>(rhs);

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Table name
  if (table_name_ != other.table_name_) return false;

  // Schema
  if (table_schema_ != nullptr) {
    if (other.table_schema_ == nullptr) return false;
    if (*table_schema_ != *other.table_schema_) return false;
  }
  if (table_schema_ == nullptr && other.table_schema_ != nullptr) return false;

  // Has primary key
  if (has_primary_key_ != other.has_primary_key_) return false;

  // Primary Key
  if (has_primary_key_ && (primary_key_ != other.primary_key_)) return false;

  // Foreign key
  if (foreign_keys_ != other.foreign_keys_) return false;

  // Unique constraints
  if (con_uniques_ != other.con_uniques_) return false;

  // Check constraints
  if (con_checks_ != other.con_checks_) return false;

  return true;
}

nlohmann::json CreateTablePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["namespace_oid"] = namespace_oid_;
  j["table_name"] = table_name_;
  j["table_schema"] = table_schema_->ToJson();

  j["has_primary_key"] = has_primary_key_;
  if (has_primary_key_) {
    j["primary_key"] = primary_key_;
  }

  j["foreign_keys"] = foreign_keys_;
  j["con_uniques"] = con_uniques_;
  j["con_checks"] = con_checks_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateTablePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
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

  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(PrimaryKeyInfo);
DEFINE_JSON_BODY_DECLARATIONS(ForeignKeyInfo);
DEFINE_JSON_BODY_DECLARATIONS(UniqueInfo);
DEFINE_JSON_BODY_DECLARATIONS(CheckInfo);
DEFINE_JSON_BODY_DECLARATIONS(CreateTablePlanNode);

}  // namespace noisepage::planner
