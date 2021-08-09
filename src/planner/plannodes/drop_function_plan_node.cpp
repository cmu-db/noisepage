#include "planner/plannodes/drop_function_plan_node.h"

#include <memory>
#include <string>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropFunctionPlanNode> DropFunctionPlanNode::Builder::Build() {
  return std::unique_ptr<DropFunctionPlanNode>(new DropFunctionPlanNode(std::move(children_), std::move(output_schema_),
                                                                        database_oid_, proc_oid_, plan_node_id_));
}

DropFunctionPlanNode::DropFunctionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                           std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                                           catalog::proc_oid_t proc_oid, plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id),
      database_oid_(database_oid),
      proc_oid_(proc_oid) {}

common::hash_t DropFunctionPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();
  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  // Hash procedure oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(proc_oid_));
  return hash;
}

bool DropFunctionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropFunctionPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (proc_oid_ != other.proc_oid_) return false;

  return true;
}

nlohmann::json DropFunctionPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["proc_oid"] = proc_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropFunctionPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  proc_oid_ = j.at("proc_oid").get<catalog::proc_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DropFunctionPlanNode);

}  // namespace noisepage::planner
