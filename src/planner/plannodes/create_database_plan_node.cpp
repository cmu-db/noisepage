#include "planner/plannodes/create_database_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t CreateDatabasePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));

  return hash;
}

bool CreateDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateDatabasePlanNode &>(rhs);

  // Database name
  if (database_name_ != other.database_name_) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json CreateDatabasePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_name"] = database_name_;
  return j;
}

void CreateDatabasePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_name_ = j.at("database_name").get<std::string>();
}

}  // namespace terrier::planner
