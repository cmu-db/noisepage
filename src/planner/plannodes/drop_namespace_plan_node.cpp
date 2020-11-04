#include "planner/plannodes/drop_namespace_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropNamespacePlanNode> DropNamespacePlanNode::Builder::Build() {
  return std::unique_ptr<DropNamespacePlanNode>(
      new DropNamespacePlanNode(std::move(children_), std::move(output_schema_), namespace_oid_));
}

DropNamespacePlanNode::DropNamespacePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                             std::unique_ptr<OutputSchema> output_schema,
                                             catalog::namespace_oid_t namespace_oid)
    : AbstractPlanNode(std::move(children), std::move(output_schema)), namespace_oid_(namespace_oid) {}

common::hash_t DropNamespacePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  return hash;
}

bool DropNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropNamespacePlanNode &>(rhs);

  return namespace_oid_ == other.namespace_oid_;
}

nlohmann::json DropNamespacePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["namespace_oid"] = namespace_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropNamespacePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DropNamespacePlanNode);

}  // namespace noisepage::planner
