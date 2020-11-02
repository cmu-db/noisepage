#include "planner/plannodes/create_database_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<CreateDatabasePlanNode> CreateDatabasePlanNode::Builder::Build() {
  return std::unique_ptr<CreateDatabasePlanNode>(
      new CreateDatabasePlanNode(std::move(children_), std::move(output_schema_), std::move(database_name_)));
}

CreateDatabasePlanNode::CreateDatabasePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                               std::unique_ptr<OutputSchema> output_schema, std::string database_name)
    : AbstractPlanNode(std::move(children), std::move(output_schema)), database_name_(std::move(database_name)) {}

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
