#include "planner/plannodes/drop_sequence_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t DropSequencePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_oid_));

  return hash;
}

bool DropSequencePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropSequencePlanNode &>(rhs);

  return sequence_oid_ == other.sequence_oid_;
}

nlohmann::json DropSequencePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["sequence_oid"] = sequence_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropSequencePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  sequence_oid_ = j.at("sequence_oid").get<catalog::sequence_oid_t>();
  return exprs;
}

}  // namespace terrier::planner
