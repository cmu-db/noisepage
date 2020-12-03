#include "planner/plannodes/drop_index_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropIndexPlanNode> DropIndexPlanNode::Builder::Build() {
  return std::unique_ptr<DropIndexPlanNode>(
      new DropIndexPlanNode(std::move(children_), std::move(output_schema_), index_oid_, plan_node_id_));
}

DropIndexPlanNode::DropIndexPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                     std::unique_ptr<OutputSchema> output_schema, catalog::index_oid_t index_oid,
                                     plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id), index_oid_(index_oid) {}

common::hash_t DropIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  return hash;
}

bool DropIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropIndexPlanNode &>(rhs);

  return index_oid_ == other.index_oid_;
}

nlohmann::json DropIndexPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropIndexPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(DropIndexPlanNode);

}  // namespace noisepage::planner
