#include "planner/plannodes/seq_scan_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<SeqScanPlanNode> SeqScanPlanNode::Builder::Build() {
  return std::unique_ptr<SeqScanPlanNode>(new SeqScanPlanNode(
      std::move(children_), std::move(output_schema_), scan_predicate_, std::move(column_oids_), is_for_update_,
      database_oid_, table_oid_, scan_limit_, scan_has_limit_, scan_offset_, scan_has_offset_));
}

SeqScanPlanNode::SeqScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                 std::unique_ptr<OutputSchema> output_schema,
                                 common::ManagedPointer<parser::AbstractExpression> predicate,
                                 std::vector<catalog::col_oid_t> &&column_oids, bool is_for_update,
                                 catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, uint32_t scan_limit,
                                 bool scan_has_limit, uint32_t scan_offset, bool scan_has_offset)
    : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate, is_for_update, database_oid,
                           scan_limit, scan_has_limit, scan_offset, scan_has_offset),
      column_oids_(std::move(column_oids)),
      table_oid_(table_oid) {}

common::hash_t SeqScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, column_oids_.begin(), column_oids_.end());
  return hash;
}

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  auto &other = static_cast<const SeqScanPlanNode &>(rhs);
  if (!AbstractScanPlanNode::operator==(rhs)) return false;
  if (table_oid_ != other.table_oid_) return false;
  return column_oids_ == other.column_oids_;
}

nlohmann::json SeqScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["column_oids"] = column_oids_;
  j["table_oid"] = table_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> SeqScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractScanPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  return exprs;
}
DEFINE_JSON_BODY_DECLARATIONS(SeqScanPlanNode);

}  // namespace noisepage::planner
