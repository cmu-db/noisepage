#include "planner/plannodes/index_scan_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::planner {

common::hash_t IndexScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Index Oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  hash = common::HashUtil::CombineHashInRange(hash, column_oids_.begin(), column_oids_.end());

  return hash;
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const IndexScanPlanNode &>(rhs);

  if (column_oids_ != other.column_oids_) return false;

  // Index Oid
  return (index_oid_ == other.index_oid_);
}

nlohmann::json IndexScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  j["column_oids"] = column_oids_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> IndexScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractScanPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(IndexScanPlanNode);

}  // namespace noisepage::planner
