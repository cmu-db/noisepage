#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_scan_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for an index scan
 */
class IndexScanPlanNode : public AbstractScanPlanNode {
 public:
  /**
   * Builder for an index scan plan node
   */
  class Builder : public AbstractScanPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param oid oid for index to use for scan
     * @return builder object
     */
    Builder &SetIndexOid(catalog::index_oid_t oid) {
      index_oid_ = oid;
      return *this;
    }

    /**
     * @param oid oid of the table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t oid) {
      table_oid_ = oid;
      return *this;
    }

    /**
     * Sets the scan type
     */
    Builder &SetScanType(IndexScanType scan_type) {
      scan_type_ = scan_type;
      return *this;
    }

    /**
     * Sets the index size
     */
    Builder &SetIndexSize(uint64_t index_size) {
      index_size_ = index_size;
      return *this;
    }

    /**
     * Sets the index cols.
     */
    Builder &AddIndexColumn(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      lo_index_cols_.emplace(col_oid, expr);
      return *this;
    }

    /**
     * Sets the lower bound index cols.
     */
    Builder &AddLoIndexColumn(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      lo_index_cols_.emplace(col_oid, expr);
      return *this;
    }

    /**
     * Sets the index upper bound cols.
     */
    Builder &AddHiIndexColumn(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      hi_index_cols_.emplace(col_oid, expr);
      return *this;
    }

    /**
     * @param column_oids OIDs of columns to scan
     * @return builder object
     */
    Builder &SetColumnOids(std::vector<catalog::col_oid_t> &&column_oids) {
      column_oids_ = column_oids;
      return *this;
    }

    /**
     * @param table_num_tuple estimated number of tuples in the table
     * @return builder object
     */
    Builder &SetTableNumTuple(uint64_t table_num_tuple) {
      table_num_tuple_ = table_num_tuple;
      return *this;
    }

    /**
     * @param cover_all_columns whether the index covers all predicate columns
     * @return builder object
     */
    Builder &SetCoverAllColumns(bool cover_all_columns) {
      cover_all_columns_ = cover_all_columns;
      return *this;
    }

    /**
     * Build the Index scan plan node
     * @return plan node
     */
    std::unique_ptr<IndexScanPlanNode> Build();

   private:
    IndexScanType scan_type_;
    catalog::index_oid_t index_oid_;
    catalog::table_oid_t table_oid_;
    std::vector<catalog::col_oid_t> column_oids_;
    uint64_t table_num_tuple_{0};
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
    uint64_t index_size_{0};
    bool cover_all_columns_{false};
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param column_oids OIDs of columns to scan
   * @param is_for_update scan is used for an update
   * @param database_oid database oid for scan
   * @param index_oid OID of index to be used in index scan
   * @param table_oid OID of the table
   * @param scan_type Type of the scan
   * @param lo_index_cols lower bound of the scan (or exact key when scan type = Exact).
   * @param hi_index_cols upper bound of the scan
   * @param index_size number of tuples in index
   * @param cover_all_columns whether the index covers all predicate columns
   * @param plan_node_id Plan node id
   */
  IndexScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema,
                    common::ManagedPointer<parser::AbstractExpression> predicate,
                    std::vector<catalog::col_oid_t> &&column_oids, bool is_for_update, catalog::db_oid_t database_oid,
                    catalog::index_oid_t index_oid, catalog::table_oid_t table_oid, IndexScanType scan_type,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_index_cols,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_index_cols,
                    uint32_t scan_limit, bool scan_has_limit, uint32_t scan_offset, bool scan_has_offset,
                    uint64_t index_size, uint64_t table_num_tuple, bool cover_all_columns, plan_node_id_t plan_node_id);

 public:
  /**
   * Default constructor used for deserialization
   */
  IndexScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(IndexScanPlanNode)

  /**
   * @return index OID to be used for scan
   */
  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  /**
   * @return the OID of the table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return The scan type
   */
  IndexScanType GetScanType() const { return scan_type_; }

  /**
   * @return OIDs of columns to scan
   */
  const std::vector<catalog::col_oid_t> &GetColumnOids() const { return column_oids_; }

  /**
   * @return the index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetIndexColumns() const {
    return lo_index_cols_;
  }

  /**
   * @return the lower bound index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetLoIndexColumns() const {
    return lo_index_cols_;
  }

  /**
   * @return the upper bound index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetHiIndexColumns() const {
    return hi_index_cols_;
  }

  /**
   * @return the estimation for the number of tuples in the underlying table
   */
  uint64_t GetTableNumTuple() const { return table_num_tuple_; }

  /**
   * @return index size
   */
  uint64_t GetIndexSize() const { return index_size_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXSCAN; }

  /**
   * @return whether the index covers all predicate columns
   */
  bool GetCoverAllColumns() const { return cover_all_columns_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  IndexScanType scan_type_;
  catalog::index_oid_t index_oid_;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> column_oids_;
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
  uint64_t table_num_tuple_;
  uint64_t index_size_;
  bool cover_all_columns_;
};

DEFINE_JSON_HEADER_DECLARATIONS(IndexScanPlanNode);

}  // namespace noisepage::planner
