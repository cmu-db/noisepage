#include "planner/plannodes/index_join_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::planner {

common::hash_t IndexJoinPlanNode::Hash() const { return AbstractJoinPlanNode::Hash(); }

bool IndexJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const IndexJoinPlanNode &>(rhs);
  return other.table_oid_ == table_oid_ && other.index_oid_ == index_oid_;
}

nlohmann::json IndexJoinPlanNode::ToJson() const {
  nlohmann::json j = AbstractJoinPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  j["table_oid"] = table_oid_;
  j["scan_type"] = scan_type_;
  j["lo_index_cols"] = lo_index_cols_;
  j["hi_index_cols"] = hi_index_cols_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> IndexJoinPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractJoinPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(IndexJoinPlanNode);

}  // namespace noisepage::planner
