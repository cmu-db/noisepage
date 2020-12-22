#include "planner/plannodes/index_join_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<IndexJoinPlanNode> IndexJoinPlanNode::Builder::Build() {
  return std::unique_ptr<IndexJoinPlanNode>(new IndexJoinPlanNode(
      std::move(children_), std::move(output_schema_), join_type_, join_predicate_, index_oid_, table_oid_, scan_type_,
      std::move(lo_index_cols_), std::move(hi_index_cols_), index_size_, plan_node_id_));
}

IndexJoinPlanNode::IndexJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                     std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                                     common::ManagedPointer<parser::AbstractExpression> predicate,
                                     catalog::index_oid_t index_oid, catalog::table_oid_t table_oid,
                                     IndexScanType scan_type,
                                     std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_cols,
                                     std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_cols,
                                     uint64_t index_size, plan_node_id_t plan_node_id)
    : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate, plan_node_id),
      index_oid_(index_oid),
      table_oid_(table_oid),
      scan_type_(scan_type),
      lo_index_cols_(std::move(lo_cols)),
      hi_index_cols_(std::move(hi_cols)),
      index_size_(index_size) {}

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

std::vector<catalog::col_oid_t> IndexJoinPlanNode::CollectInputOids() const {
  std::vector<catalog::col_oid_t> result;
  // Scan predicate
  if (GetJoinPredicate() != nullptr) CollectOids(&result, GetJoinPredicate().Get());
  // Output expressions
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    CollectOids(&result, col.GetExpr().Get());
  }
  // Remove duplicates
  std::unordered_set<catalog::col_oid_t> s(result.begin(), result.end());
  result.assign(s.begin(), s.end());
  return result;
}

DEFINE_JSON_BODY_DECLARATIONS(IndexJoinPlanNode);

}  // namespace noisepage::planner
