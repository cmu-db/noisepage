#include "planner/plannodes/drop_table_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropTablePlanNode> DropTablePlanNode::Builder::Build() {
  return std::unique_ptr<DropTablePlanNode>(
      new DropTablePlanNode(std::move(children_), std::move(output_schema_), table_oid_, plan_node_id_));
}

DropTablePlanNode::DropTablePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                     std::unique_ptr<OutputSchema> output_schema, catalog::table_oid_t table_oid,
                                     plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id), table_oid_(table_oid) {}

common::hash_t DropTablePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  return hash;
}

bool DropTablePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropTablePlanNode &>(rhs);

  // Table OID
  return table_oid_ == other.table_oid_;
}

nlohmann::json DropTablePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["table_oid"] = table_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropTablePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(DropTablePlanNode);

}  // namespace noisepage::planner
