#include "planner/plannodes/abstract_join_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

AbstractJoinPlanNode::AbstractJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                           std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                                           common::ManagedPointer<parser::AbstractExpression> predicate)
    : AbstractPlanNode(std::move(children), std::move(output_schema)),
      join_type_(join_type),
      join_predicate_(predicate) {}

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AbstractJoinPlanNode &>(rhs);

  // Check join type
  if (join_type_ != other.join_type_) return false;

  // Check predicate
  if ((join_predicate_ == nullptr && other.join_predicate_ != nullptr) ||
      (join_predicate_ != nullptr && other.join_predicate_ == nullptr)) {
    return false;
  }
  if (join_predicate_ != nullptr && *join_predicate_ != *other.join_predicate_) {
    return false;
  }

  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Join Type
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(join_type_));

  // Predicate
  if (join_predicate_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
  }

  return hash;
}

nlohmann::json AbstractJoinPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["join_type"] = join_type_;
  j["join_predicate"] = join_predicate_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AbstractJoinPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  join_type_ = j.at("join_type").get<LogicalJoinType>();
  if (!j.at("join_predicate").is_null()) {
    auto deserialized = parser::DeserializeExpression(j.at("join_predicate"));
    join_predicate_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  return exprs;
}

}  // namespace noisepage::planner
