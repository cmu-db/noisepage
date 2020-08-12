#include "catalog/schema.h"
#include "common/json.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::catalog {

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

Schema::Schema(std::vector<std::string> column_aliases, std::vector<type::TypeId> column_types) {
  std::vector<Column> columns;
  for (uint32_t i = 0; i < column_aliases.size(); i++) {
    columns.emplace_back(column_aliases[i], column_types[i], false, parser::ConstantValueExpression(column_types[i]),
                         TEMP_OID(catalog::col_oid_t, i));
  }

  columns_ = std::move(columns);
  for (uint32_t i = 0; i < columns_.size(); i++) {
    // If not all columns assigned OIDs, then clear the map because this is
    // a definition of a new/modified table not a catalog generated schema.
    if (columns_[i].Oid() == catalog::INVALID_COLUMN_OID) {
      col_oid_to_offset_.clear();
      return;
    }
    col_oid_to_offset_[columns_[i].Oid()] = i;
  }
}

DEFINE_JSON_BODY_DECLARATIONS(Schema::Column);
DEFINE_JSON_BODY_DECLARATIONS(Schema);

}  // namespace terrier::catalog
