#include "planner/plannodes/create_database_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t CreateDatabasePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));

  return hash;
}

bool CreateDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateDatabasePlanNode &>(rhs);

  return database_name_ == other.database_name_;
}

nlohmann::json CreateDatabasePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_name"] = database_name_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateDatabasePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_name_ = j.at("database_name").get<std::string>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(CreateDatabasePlanNode);

}  // namespace noisepage::planner
