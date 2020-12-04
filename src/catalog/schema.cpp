#include "catalog/schema.h"

#include "common/json.h"

namespace noisepage::catalog {

nlohmann::json Schema::Column::ToJson() const {
  nlohmann::json j;
  j["name"] = name_;
  j["type"] = type_;
  j["attr_length"] = attr_length_;
  j["type_modifier"] = type_modifier_;
  j["nullable"] = nullable_;
  j["oid"] = oid_;
  j["default_value"] = default_value_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> Schema::Column::FromJson(const nlohmann::json &j) {
  name_ = j.at("name").get<std::string>();
  type_ = j.at("type").get<type::TypeId>();
  attr_length_ = j.at("attr_length").get<uint16_t>();
  type_modifier_ = j.at("type_modifier").get<int32_t>();
  nullable_ = j.at("nullable").get<bool>();
  oid_ = j.at("oid").get<col_oid_t>();
  auto deserialized = parser::DeserializeExpression(j.at("default_value"));
  default_value_ = std::move(deserialized.result_);
  return std::move(deserialized.non_owned_exprs_);
}

nlohmann::json Schema::ToJson() const {
  // Only need to serialize columns_ because col_oid_to_offset is derived from columns_
  nlohmann::json j;
  j["columns"] = columns_;
  return j;
}

void Schema::FromJson(const nlohmann::json &j) {
  NOISEPAGE_ASSERT(false, "Schema::FromJson should never be invoked directly; use DeserializeSchema");
}

std::unique_ptr<Schema> Schema::DeserializeSchema(const nlohmann::json &j) {
  auto columns = j.at("columns").get<std::vector<Schema::Column>>();
  return std::make_unique<Schema>(columns);
}

DEFINE_JSON_BODY_DECLARATIONS(Schema::Column);
DEFINE_JSON_BODY_DECLARATIONS(Schema);

}  // namespace noisepage::catalog
