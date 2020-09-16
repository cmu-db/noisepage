#include "catalog/index_schema.h"

#include "common/json.h"

namespace terrier::catalog {

nlohmann::json IndexSchema::Column::ToJson() const {
  nlohmann::json j;
  j["name"] = name_;
  j["type"] = Type();
  j["max_varlen_size"] = MaxVarlenSize();
  j["nullable"] = Nullable();
  j["oid"] = oid_;
  j["definition"] = definition_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> IndexSchema::Column::FromJson(const nlohmann::json &j) {
  name_ = j.at("name").get<std::string>();
  SetTypeId(j.at("type").get<type::TypeId>());
  SetMaxVarlenSize(j.at("max_varlen_size").get<uint16_t>());
  SetNullable(j.at("nullable").get<bool>());
  SetOid(j.at("oid").get<indexkeycol_oid_t>());
  auto deserialized = parser::DeserializeExpression(j.at("definition"));
  definition_ = std::move(deserialized.result_);
  return std::move(deserialized.non_owned_exprs_);
}

nlohmann::json IndexSchema::ToJson() const {
  // Only need to serialize columns_ because col_oid_to_offset is derived from columns_
  nlohmann::json j;
  j["columns"] = columns_;
  j["type"] = static_cast<char>(type_);
  j["unique"] = is_unique_;
  j["primary"] = is_primary_;
  j["exclusion"] = is_exclusion_;
  j["immediate"] = is_immediate_;
  return j;
}

void IndexSchema::FromJson(const nlohmann::json &j) {
  TERRIER_ASSERT(false, "Schema::FromJson should never be invoked directly; use DeserializeSchema");
}

std::unique_ptr<IndexSchema> IndexSchema::DeserializeSchema(const nlohmann::json &j) {
  auto columns = j.at("columns").get<std::vector<IndexSchema::Column>>();

  auto unique = j.at("unique").get<bool>();
  auto primary = j.at("primary").get<bool>();
  auto exclusion = j.at("exclusion").get<bool>();
  auto immediate = j.at("immediate").get<bool>();
  auto type = static_cast<storage::index::IndexType>(j.at("type").get<char>());

  auto schema = std::make_unique<IndexSchema>(columns, type, unique, primary, exclusion, immediate);

  return schema;
}

DEFINE_JSON_BODY_DECLARATIONS(IndexSchema::Column);
DEFINE_JSON_BODY_DECLARATIONS(IndexSchema);

}  // namespace terrier::catalog
