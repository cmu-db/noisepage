#include "parser/table_ref.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::parser {

/**
 * @return JoinDefinition serialized to json
 */
nlohmann::json JoinDefinition::ToJson() const {
  nlohmann::json j;
  j["type"] = type_;
  j["left"] = left_;
  j["right"] = right_;
  j["condition"] = condition_;
  return j;
}

/**
 * @param j json to deserialize
 */
void JoinDefinition::FromJson(const nlohmann::json &j) {
  // Deserialize type
  type_ = j.at("type").get<JoinType>();

  // Deserialize left
  if (!j.at("left").is_null()) {
    // TODO(WAN): lifetime
    left_ = common::ManagedPointer(new TableRef());
    left_->FromJson(j.at("left"));
  }

  // Deserialize right
  if (!j.at("right").is_null()) {
    // TODO(WAN): lifetime
    right_ = common::ManagedPointer(new TableRef());
    right_->FromJson(j.at("right"));
  }

  // Deserialize condition
  if (!j.at("condition").is_null()) {
    // TODO(WAN): lifetime
    condition_ = common::ManagedPointer(DeserializeExpression(j.at("condition")));
  }
}

nlohmann::json TableRef::ToJson() const {
  nlohmann::json j;
  j["type"] = type_;
  j["alias"] = alias_;
  j["table_info"] = table_info_;
  j["select"] = select_;
  j["list"] = list_;
  j["join"] = join_;
  return j;
}

void TableRef::FromJson(const nlohmann::json &j) {
  // Deserialize type
  type_ = j.at("type").get<TableReferenceType>();

  // Deserialize alias
  alias_ = j.at("alias").get<std::string>();

  // Deserialize table info
  if (!j.at("table_info").is_null()) {
    // TODO(WAN): lifetime
    table_info_ = common::ManagedPointer(new TableInfo());
    table_info_->FromJson(j.at("table_info"));
  }

  // Deserialize select
  if (!j.at("select").is_null()) {
    // TODO(WAN): lifetime
    select_ = common::ManagedPointer(new SelectStatement());
    select_->FromJson(j.at("select"));
  }

  // Deserialize list
  auto list_jsons = j.at("list").get<std::vector<nlohmann::json>>();
  for (const auto &list_json : list_jsons) {
    // TODO(WAN): lifetime
    auto ref = common::ManagedPointer(new TableRef());
    ref->FromJson(list_json);
    list_.push_back(ref);
  }

  // Deserialize join
  if (!j.at("join").is_null()) {
    // TODO(WAN): lifetime
    join_ = common::ManagedPointer(new JoinDefinition());
    join_->FromJson(j.at("join"));
  }
}

}  // namespace terrier::parser
