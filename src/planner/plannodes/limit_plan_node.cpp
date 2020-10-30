#include "planner/plannodes/limit_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<LimitPlanNode> LimitPlanNode::Builder::Build() {
  return std::unique_ptr<LimitPlanNode>(
      new LimitPlanNode(std::move(children_), std::move(output_schema_), limit_, offset_));
}

LimitPlanNode::LimitPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                             std::unique_ptr<OutputSchema> output_schema, size_t limit, size_t offset)
    : AbstractPlanNode(std::move(children), std::move(output_schema)), limit_(limit), offset_(offset) {}

common::hash_t LimitPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Limit
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));

  // Offset
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));

  return hash;
}

bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const LimitPlanNode &>(rhs);

  // Limit
  if (limit_ != other.limit_) return false;

  // Offset
  if (offset_ != other.offset_) return false;

  return true;
}

nlohmann::json LimitPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["limit"] = limit_;
  j["offset"] = offset_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> LimitPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  limit_ = j.at("limit").get<size_t>();
  offset_ = j.at("offset").get<size_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(LimitPlanNode);

}  // namespace noisepage::planner
