#include "planner/plannodes/update_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/abstract_scan_plan_node.h"

namespace noisepage::planner {

common::hash_t UpdatePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash update_primary_key
  auto is_update_primary_key = GetUpdatePrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_update_primary_key));

  // SET Clauses
  for (const auto &set : sets_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(set.first));
    if (set.second != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, set.second->Hash());
    }
  }

  for (const auto &index_oid : index_oids_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid));
  }

  return hash;
}

bool UpdatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const UpdatePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Table OID
  if (table_oid_ != other.table_oid_) return false;

  // Update primary key
  if (update_primary_key_ != other.update_primary_key_) return false;

  if (sets_.size() != other.sets_.size()) return false;
  for (size_t idx = 0; idx < sets_.size(); idx++) {
    if (sets_[idx].first != other.sets_[idx].first) return false;

    if ((sets_[idx].second == nullptr && other.sets_[idx].second != nullptr) ||
        (sets_[idx].second != nullptr && other.sets_[idx].second == nullptr))
      return false;
    if (sets_[idx].second != nullptr && *sets_[idx].second != *other.sets_[idx].second) return false;
  }

  if (index_oids_.size() != other.index_oids_.size()) return false;
  for (int i = 0; i < static_cast<int>(index_oids_.size()); i++) {
    if (index_oids_[i] != other.index_oids_[i]) return false;
  }
  return true;
}

nlohmann::json UpdatePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;
  j["update_primary_key"] = update_primary_key_;

  std::vector<std::pair<catalog::col_oid_t, nlohmann::json>> sets;
  sets.reserve(sets_.size());
  for (const auto &set : sets_) {
    sets.emplace_back(set.first, set.second->ToJson());
  }
  j["sets"] = sets;
  j["index_oids"] = index_oids_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> UpdatePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  update_primary_key_ = j.at("update_primary_key").get<bool>();

  auto sets = j.at("sets").get<std::vector<std::pair<catalog::col_oid_t, nlohmann::json>>>();
  for (const auto &key_json : sets) {
    auto deserialized = parser::DeserializeExpression(key_json.second);
    sets_.emplace_back(key_json.first, common::ManagedPointer(deserialized.result_));
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  index_oids_ = j.at("index_oids").get<std::vector<catalog::index_oid_t>>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(UpdatePlanNode);

}  // namespace noisepage::planner
