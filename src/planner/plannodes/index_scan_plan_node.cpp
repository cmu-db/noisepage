#include "planner/plannodes/index_scan_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<IndexScanPlanNode> IndexScanPlanNode::Builder::Build() {
  return std::unique_ptr<IndexScanPlanNode>(new IndexScanPlanNode(
      std::move(children_), std::move(output_schema_), scan_predicate_, std::move(column_oids_), is_for_update_,
      database_oid_, index_oid_, table_oid_, scan_type_, std::move(lo_index_cols_), std::move(hi_index_cols_),
      scan_limit_, scan_has_limit_, scan_offset_, scan_has_offset_, index_size_, table_num_tuple_, cover_all_columns_,
      plan_node_id_));
}

IndexScanPlanNode::IndexScanPlanNode(
    std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
    common::ManagedPointer<parser::AbstractExpression> predicate, std::vector<catalog::col_oid_t> &&column_oids,
    bool is_for_update, catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, catalog::table_oid_t table_oid,
    IndexScanType scan_type, std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_index_cols,
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_index_cols, uint32_t scan_limit,
    bool scan_has_limit, uint32_t scan_offset, bool scan_has_offset, uint64_t index_size, uint64_t table_num_tuple,
    bool cover_all_columns, plan_node_id_t plan_node_id)
    : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate, is_for_update, database_oid,
                           scan_limit, scan_has_limit, scan_offset, scan_has_offset, plan_node_id),
      scan_type_(scan_type),
      index_oid_(index_oid),
      table_oid_(table_oid),
      column_oids_(column_oids),
      lo_index_cols_(std::move(lo_index_cols)),
      hi_index_cols_(std::move(hi_index_cols)),
      table_num_tuple_(table_num_tuple),
      index_size_(index_size),
      cover_all_columns_(cover_all_columns) {}

common::hash_t IndexScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Index Oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  hash = common::HashUtil::CombineHashInRange(hash, column_oids_.begin(), column_oids_.end());

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(cover_all_columns_));

  return hash;
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const IndexScanPlanNode &>(rhs);

  if (column_oids_ != other.column_oids_) return false;

  if (cover_all_columns_ != other.cover_all_columns_) return false;

  // Index Oid
  return (index_oid_ == other.index_oid_);
}

nlohmann::json IndexScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  j["column_oids"] = column_oids_;
  j["cover_all_columns"] = cover_all_columns_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> IndexScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractScanPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
  cover_all_columns_ = j.at("cover_all_columns").get<bool>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(IndexScanPlanNode);

}  // namespace noisepage::planner
