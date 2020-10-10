#include "parser/table_ref.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "parser/select_statement.h"

namespace terrier::parser {

/**
 * @return JoinDefinition serialized to json
 */
nlohmann::json JoinDefinition::ToJson() const {
  nlohmann::json j;
  j["type"] = type_;
  j["left"] = left_->ToJson();
  j["right"] = right_->ToJson();
  j["condition"] = condition_->ToJson();
  return j;
}

common::hash_t JoinDefinition::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(type_);
  if (left_ != nullptr) hash = common::HashUtil::CombineHashes(hash, left_->Hash());
  if (right_ != nullptr) hash = common::HashUtil::CombineHashes(hash, right_->Hash());
  if (condition_ != nullptr) hash = common::HashUtil::CombineHashes(hash, condition_->Hash());
  return hash;
}

bool JoinDefinition::operator==(const JoinDefinition &rhs) const {
  if (type_ != rhs.type_) return false;
  if (left_ != nullptr && rhs.left_ == nullptr) return false;
  if (left_ == nullptr && rhs.left_ != nullptr) return false;
  if (left_ != nullptr && rhs.left_ != nullptr && *(left_) != *(rhs.left_)) return false;

  if (right_ != nullptr && rhs.right_ == nullptr) return false;
  if (right_ == nullptr && rhs.right_ != nullptr) return false;
  if (right_ != nullptr && rhs.right_ != nullptr && *(right_) != *(rhs.right_)) return false;

  if (condition_ != nullptr && rhs.condition_ == nullptr) return false;
  if (condition_ == nullptr && rhs.condition_ != nullptr) return false;
  if (condition_ != nullptr && rhs.condition_ != nullptr && *(condition_) != *(rhs.condition_)) return false;
  return true;
}

/**
 * @param j json to deserialize
 */
std::vector<std::unique_ptr<AbstractExpression>> JoinDefinition::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  // Deserialize type
  type_ = j.at("type").get<JoinType>();

  // Deserialize left
  if (!j.at("left").is_null()) {
    left_ = std::make_unique<TableRef>();
    auto e1 = left_->FromJson(j.at("left"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize right
  if (!j.at("right").is_null()) {
    right_ = std::make_unique<TableRef>();
    auto e1 = right_->FromJson(j.at("right"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize condition
  if (!j.at("condition").is_null()) {
    auto deserialized = DeserializeExpression(j.at("condition"));
    condition_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(JoinDefinition);

std::unique_ptr<JoinDefinition> JoinDefinition::Copy() {
  return std::make_unique<JoinDefinition>(type_, left_->Copy(), right_->Copy(), condition_);
}

nlohmann::json TableRef::ToJson() const {
  nlohmann::json j;
  j["type"] = type_;
  j["alias"] = alias_;
  j["table_info"] = table_info_ == nullptr ? nlohmann::json(nullptr) : table_info_->ToJson();
  j["select"] = select_ == nullptr ? nlohmann::json(nullptr) : select_->ToJson();
  std::vector<nlohmann::json> list;
  list.reserve(list_.size());
  for (const auto &item : list_) {
    list.emplace_back(item->ToJson());
  }
  j["list"] = list;
  j["join"] = join_ == nullptr ? nlohmann::json(nullptr) : join_->ToJson();
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> TableRef::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  // Deserialize type
  type_ = j.at("type").get<TableReferenceType>();

  // Deserialize alias
  alias_ = j.at("alias").get<std::string>();

  // Deserialize table info
  if (!j.at("table_info").is_null()) {
    table_info_ = std::make_unique<TableInfo>();
    auto e1 = table_info_->FromJson(j.at("table_info"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize select
  if (!j.at("select").is_null()) {
    select_ = std::make_unique<parser::SelectStatement>();
    auto e1 = select_->FromJson(j.at("select"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize list
  auto list_jsons = j.at("list").get<std::vector<nlohmann::json>>();
  for (const auto &list_json : list_jsons) {
    auto ref = std::make_unique<TableRef>();
    auto e1 = ref->FromJson(list_json);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    list_.emplace_back(std::move(ref));
  }

  // Deserialize join
  if (!j.at("join").is_null()) {
    join_ = std::make_unique<JoinDefinition>();
    auto e1 = join_->FromJson(j.at("join"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(TableRef);

common::hash_t TableRef::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(type_);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(alias_));
  if (table_info_ != nullptr) hash = common::HashUtil::CombineHashes(hash, table_info_->Hash());
  if (select_ != nullptr) hash = common::HashUtil::CombineHashes(hash, select_->Hash());
  if (join_ != nullptr) hash = common::HashUtil::CombineHashes(hash, join_->Hash());
  for (const auto &tb : list_) {
    hash = common::HashUtil::CombineHashes(hash, tb->Hash());
  }
  return hash;
}

bool TableRef::operator==(const TableRef &rhs) const {
  if (type_ != rhs.type_) return false;
  if (alias_ != rhs.alias_) return false;
  if (table_info_ != nullptr && rhs.table_info_ == nullptr) return false;
  if (table_info_ == nullptr && rhs.table_info_ != nullptr) return false;
  if (table_info_ != nullptr && rhs.table_info_ != nullptr && *(table_info_) != *(rhs.table_info_)) return false;

  if (select_ != nullptr && rhs.select_ == nullptr) return false;
  if (select_ == nullptr && rhs.select_ != nullptr) return false;
  if (select_ != nullptr && rhs.select_ != nullptr && *(select_) != *(rhs.select_)) return false;

  if (join_ != nullptr && rhs.join_ == nullptr) return false;
  if (join_ == nullptr && rhs.join_ != nullptr) return false;
  if (join_ != nullptr && rhs.join_ != nullptr && *(join_) != *(rhs.join_)) return false;

  if (list_.size() != rhs.list_.size()) return false;
  for (size_t i = 0; i < list_.size(); i++)
    if (*(list_[i]) != *(rhs.list_[i])) return false;
  return true;
}

std::unique_ptr<TableRef> TableRef::Copy() {
  auto table_ref = std::make_unique<TableRef>();

  table_ref->type_ = type_;
  table_ref->alias_ = alias_;
  table_ref->table_info_ = table_info_ == nullptr ? nullptr : table_info_->Copy();
  table_ref->select_ = select_ == nullptr ? nullptr : select_->Copy();
  table_ref->join_ = join_ == nullptr ? nullptr : join_->Copy();

  table_ref->list_.reserve(list_.size());
  for (const auto &item : list_) {
    table_ref->list_.emplace_back(item->Copy());
  }
  return table_ref;
}
}  // namespace terrier::parser
