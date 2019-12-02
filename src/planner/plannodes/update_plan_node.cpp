#include "planner/plannodes/update_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "planner/plannodes/abstract_scan_plan_node.h"

namespace terrier::planner {

common::hash_t UpdatePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash update_primary_key
  auto is_indexed_update = GetIndexedUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_indexed_update));

  // SET Clauses
  for (const auto &set : sets_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(set.first));
    hash = common::HashUtil::CombineHashes(hash, set.second->Hash());
  }

  return hash;
}

bool UpdatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const UpdatePlanNode &>(rhs);

  // Table OID
  if (table_oid_ != other.table_oid_) return false;

  // Compare indexed_update
  if (indexed_update_ != other.indexed_update_) return false;

  if (sets_.size() != other.sets_.size()) return false;
  for (size_t idx = 0; idx < sets_.size(); idx++) {
    if (sets_[idx].first != other.sets_[idx].first) return false;
    if (*sets_[idx].second != *other.sets_[idx].second) return false;
  }

  return true;
}

nlohmann::json UpdatePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["table_oid"] = table_oid_;
  j["indexed_update"] = indexed_update_;

  std::vector<std::pair<catalog::col_oid_t, nlohmann::json>> sets;
  sets.reserve(sets_.size());
  for (const auto &set : sets_) {
    sets.emplace_back(set.first, set.second->ToJson());
  }
  j["sets"] = sets;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> UpdatePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  indexed_update_ = j.at("indexed_update").get<bool>();

  auto sets = j.at("sets").get<std::vector<std::pair<catalog::col_oid_t, nlohmann::json>>>();
  for (const auto &key_json : sets) {
    auto deserialized = parser::DeserializeExpression(key_json.second);
    sets_.emplace_back(key_json.first, common::ManagedPointer(deserialized.result_));
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  return exprs;
}

}  // namespace terrier::planner
