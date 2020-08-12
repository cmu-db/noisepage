#include "catalog/schema.h"

#include "common/hash_util.h"
#include "common/json.h"

namespace terrier::catalog {

hash_t Schema::Column::Hash() const {
  hash_t hash = common::HashUtil::Hash(name_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(attr_size_));
  if (attr_size_ == storage::VARLEN_COLUMN)
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(max_varlen_size_));
  else
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(0));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(nullable_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(oid_));
  if (default_value_ != nullptr) hash = common::HashUtil::CombineHashes(hash, default_value_->Hash());
  return hash;
}

nlohmann::json Schema::Column::ToJson() const {
  nlohmann::json j;
  j["name"] = name_;
  j["type"] = type_;
  j["attr_size"] = attr_size_;
  j["max_varlen_size"] = max_varlen_size_;
  j["nullable"] = nullable_;
  j["oid"] = oid_;
  j["default_value"] = default_value_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> Schema::Column::FromJson(const nlohmann::json &j) {
  name_ = j.at("name").get<std::string>();
  type_ = j.at("type").get<type::TypeId>();
  attr_size_ = j.at("attr_size").get<uint16_t>();
  max_varlen_size_ = j.at("max_varlen_size").get<uint16_t>();
  nullable_ = j.at("nullable").get<bool>();
  oid_ = j.at("oid").get<col_oid_t>();
  auto deserialized = parser::DeserializeExpression(j.at("default_value"));
  default_value_ = std::move(deserialized.result_);
  return std::move(deserialized.non_owned_exprs_);
}

hash_t Schema::Hash() const {
  hash_t hash = common::HashUtil::Hash(col_oid_to_offset_.size());
  for (const auto &col : columns_) hash = common::HashUtil::CombineHashes(hash, col.Hash());
  return hash;
}

nlohmann::json Schema::ToJson() const {
  // Only need to serialize columns_ because col_oid_to_offset is derived from columns_
  nlohmann::json j;
  j["columns"] = columns_;
  return j;
}

void Schema::FromJson(const nlohmann::json &j) {
  TERRIER_ASSERT(false, "Schema::FromJson should never be invoked directly; use DeserializeSchema");
}

std::unique_ptr<Schema> Schema::DeserializeSchema(const nlohmann::json &j) {
  auto columns = j.at("columns").get<std::vector<Schema::Column>>();
  return std::make_unique<Schema>(columns);
}

DEFINE_JSON_BODY_DECLARATIONS(Schema::Column);
DEFINE_JSON_BODY_DECLARATIONS(Schema);

}  // namespace terrier::catalog
