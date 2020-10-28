#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for nested loop joins
 * TODO(Amadou): This class is fairly similar to the IndexScan plan node. However, the translators are diffrerent.
 * Make it can be replaced by a combination of IndexScan and NLJoin?
 */
class IndexJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for nested loop join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::unique_ptr<IndexJoinPlanNode> Build() {
      return std::unique_ptr<IndexJoinPlanNode>(new IndexJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, join_predicate_, index_oid_, table_oid_,
          scan_type_, std::move(lo_index_cols_), std::move(hi_index_cols_), index_size_));
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
     * @param oid oid of the index
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

   private:
    /**
     * OID of the index
     */
    catalog::index_oid_t index_oid_;
    /**
     * OID of the corresponding table
     */
    catalog::table_oid_t table_oid_;

    IndexScanType scan_type_;
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
    uint64_t index_size_{0};
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  IndexJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                    common::ManagedPointer<parser::AbstractExpression> predicate, catalog::index_oid_t index_oid,
                    catalog::table_oid_t table_oid, IndexScanType scan_type,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_cols,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_cols, uint64_t index_size)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate),
        index_oid_(index_oid),
        table_oid_(table_oid),
        scan_type_(scan_type),
        lo_index_cols_(std::move(lo_cols)),
        hi_index_cols_(std::move(hi_cols)),
        index_size_(index_size) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  IndexJoinPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(IndexJoinPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXNLJOIN; }

  /**
   * @return index size
   */
  uint64_t GetIndexSize() const { return index_size_; }

  /**
   * @return the NestedLooped value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

  /**
   * @return the OID of the index
   */
  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  /**
   * @return the OID of the table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the scan type
   */
  IndexScanType GetScanType() const { return scan_type_; }

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
   * Collect all column oids in this expression
   * @return the vector of unique columns oids
   */
  std::vector<catalog::col_oid_t> CollectInputOids() const {
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

 private:
  void CollectOids(std::vector<catalog::col_oid_t> *result, const parser::AbstractExpression *expr) const {
    if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto column_val = static_cast<const parser::ColumnValueExpression *>(expr);
      result->emplace_back(column_val->GetColumnOid());
    } else {
      for (const auto &child : expr->GetChildren()) {
        CollectOids(result, child.Get());
      }
    }
  }

  /**
   * OID of the index
   */
  catalog::index_oid_t index_oid_;
  /**
   * OID of the corresponding table
   */
  catalog::table_oid_t table_oid_;

  IndexScanType scan_type_;
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
  uint64_t index_size_;
};

DEFINE_JSON_HEADER_DECLARATIONS(IndexJoinPlanNode);

}  // namespace noisepage::planner
