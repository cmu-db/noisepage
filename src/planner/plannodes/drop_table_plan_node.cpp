#include "planner/plannodes/drop_table_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

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
