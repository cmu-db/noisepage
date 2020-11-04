#include "planner/plannodes/drop_database_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropDatabasePlanNode> DropDatabasePlanNode::Builder::Build() {
  return std::unique_ptr<DropDatabasePlanNode>(
      new DropDatabasePlanNode(std::move(children_), std::move(output_schema_), database_oid_));
}

DropDatabasePlanNode::DropDatabasePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                           std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid)
    : AbstractPlanNode(std::move(children), std::move(output_schema)), database_oid_(database_oid) {}

common::hash_t DropDatabasePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  return hash;
}

bool DropDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropDatabasePlanNode &>(rhs);

  return database_oid_ == other.database_oid_;
}

nlohmann::json DropDatabasePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropDatabasePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(DropDatabasePlanNode);

}  // namespace noisepage::planner
