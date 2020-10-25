#include "planner/plannodes/create_namespace_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "parser/parser_defs.h"

namespace noisepage::planner {

common::hash_t CreateNamespacePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash namespace_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));

  return hash;
}

bool CreateNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateNamespacePlanNode &>(rhs);

  // Schema name
  return namespace_name_ == other.namespace_name_;
}

nlohmann::json CreateNamespacePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["namespace_name"] = namespace_name_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> CreateNamespacePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  namespace_name_ = j.at("namespace_name").get<std::string>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(CreateNamespacePlanNode);

}  // namespace noisepage::planner
