#include "planner/plannodes/insert_plan_node.h"

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/json.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::planner {

common::hash_t InsertPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash parameter_info
  for (const auto &col_oid : parameter_info_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(col_oid));
  }

  // Values
  for (const auto &vals : values_) {
    for (const auto &val : vals) {
      hash = common::HashUtil::CombineHashes(hash, val->Hash());
    }
  }

  return hash;
}

bool InsertPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const InsertPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Target table OID
  if (table_oid_ != other.table_oid_) return false;

  // Values
  if (values_.size() != other.values_.size()) return false;
  for (size_t i = 0; i < values_.size(); i++) {
    if (values_[i].size() != other.values_[i].size()) return false;

    auto &tuple = values_[i];
    auto &other_tuple = other.values_[i];
    for (size_t j = 0; j < tuple.size(); j++) {
      if (*tuple[j] != *other_tuple[j]) return false;
    }
  }

  // Parameter info
  if (parameter_info_.size() != other.parameter_info_.size()) return false;

  for (int i = 0; i < static_cast<int>(parameter_info_.size()); i++) {
    if (parameter_info_[i] != other.parameter_info_[i]) return false;
  }
  return true;
}

nlohmann::json InsertPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;

  std::vector<std::vector<nlohmann::json>> values;
  values.reserve(values_.size());
  for (const auto &tuple : values_) {
    std::vector<nlohmann::json> tuple_json;
    tuple_json.reserve(tuple.size());
    for (const auto &elem : tuple) {
      tuple_json.emplace_back(elem->ToJson());
    }
    values.emplace_back(std::move(tuple_json));
  }
  j["values"] = values;
  j["parameter_info"] = parameter_info_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> InsertPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();

  values_ = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>();
  auto values = j.at("values").get<std::vector<std::vector<nlohmann::json>>>();
  for (auto &vec : values) {
    auto tuple = std::vector<common::ManagedPointer<parser::AbstractExpression>>();
    for (const auto &json : vec) {
      auto deserialized = parser::DeserializeExpression(json);
      tuple.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
    values_.push_back(std::move(tuple));
  }

  parameter_info_ = j.at("parameter_info").get<std::vector<catalog::col_oid_t>>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(InsertPlanNode);

}  // namespace noisepage::planner
