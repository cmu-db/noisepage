#include "planner/plannodes/create_sequence_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/parser_defs.h"

namespace terrier::planner {

common::hash_t CreateSequencePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash sequence_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sequence_name_));

  return hash;
}

bool CreateSequencePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateSequencePlanNode &>(rhs);

  // Sequence name
  if (GetSequenceName() != other.GetSequenceName()) return false;

  return true;
}

nlohmann::json CreateSequencePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["sequence_name"] = sequence_name_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateSequencePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  sequence_name_ = j.at("sequence_name").get<std::string>();
  return exprs;
}

}  // namespace terrier::planner
