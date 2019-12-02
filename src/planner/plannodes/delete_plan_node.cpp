#include "planner/plannodes/delete_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

namespace terrier::planner {

// TODO(Gus,Wen) Add SetParameters

common::hash_t DeletePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  return hash;
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DeletePlanNode &>(rhs);

  // Table OID
  return table_oid_ == other.table_oid_;
}

nlohmann::json DeletePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["table_oid"] = table_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DeletePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  return exprs;
}

}  // namespace terrier::planner
