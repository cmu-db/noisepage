#include "planner/plannodes/set_op_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t SetOpPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash set_op
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(set_op_));

  return hash;
}

bool SetOpPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const SetOpPlanNode &>(rhs);

  // Set op
  return (set_op_ == other.set_op_);
}

nlohmann::json SetOpPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["set_op"] = set_op_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> SetOpPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  set_op_ = j.at("set_op").get<SetOpType>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(SetOpPlanNode);

}  // namespace noisepage::planner
