#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_scan_plan_node.h"

// TODO(Gus,Wen): IndexScanDesc had a `p_runtime_key_list` that did not have a comment explaining its use. We should
// figure that out. IndexScanDesc also had an expression type list, i dont see why this can't just be taken from the
// predicate

// TODO(Gus,Wen): plan node contained info on whether the scan was left or right open. This should be computed at
// exection time

namespace terrier::planner {

using IndexExpression = common::ManagedPointer<parser::AbstractExpression>;

/**
 * Type of the index scan.
 */
enum class IndexScanType: uint8_t {
  Exact, Ascending, Descending, AscendingLimit, DescendingLimit
};

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
     * Build the nested loop join plan node
     * @return plan node
     */
    std::unique_ptr<IndexScanPlanNode> Build() {
      return std::make_unique<IndexScanPlanNode>(std::move(children_), std::move(output_schema_), scan_predicate_,
                                                 is_for_update_, is_parallel_, database_oid_, namespace_oid_,
                                                 index_oid_, table_oid_, scan_type_, std::move(lo_index_cols_), std::move(hi_index_cols_), scan_limit_);
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
     * Sets the index cols.
     */
    Builder &AddIndexColum(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      lo_index_cols_.emplace(col_oid, expr);
      return *this;
    }

    /**
    * Sets the lower bound index cols.
    */
    Builder &AddLoIndexColum(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      lo_index_cols_.emplace(col_oid, expr);
      return *this;
    }

    /**
    * Sets the index upper bound cols.
    */
    Builder &AddHiIndexColum(catalog::indexkeycol_oid_t col_oid, const IndexExpression &expr) {
      hi_index_cols_.emplace(col_oid, expr);
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

    Builder &SetScanType(IndexScanType scan_type) {
      scan_type_ = scan_type;
      return *this;
    }

    Builder &SetScanLimit(uint32_t limit) {
      scan_limit_ = limit;
      return *this;
    }

   private:
    IndexScanType scan_type_;
    catalog::index_oid_t index_oid_;
    catalog::table_oid_t table_oid_;
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
    uint32_t scan_limit_{0};
  };

 public:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param is_parallel parallel scan flag
   * @param database_oid database oid for scan
   * @param index_oid OID of index to be used in index scan
   */
  IndexScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema,
                    common::ManagedPointer<parser::AbstractExpression> predicate, bool is_for_update, bool is_parallel,
                    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                    catalog::index_oid_t index_oid, catalog::table_oid_t table_oid,
                    IndexScanType scan_type,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_index_cols,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_index_cols,
                    uint32_t scan_limit)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), std::move(predicate), is_for_update,
                             is_parallel, database_oid, namespace_oid),
        scan_type_(scan_type),
        index_oid_(index_oid),
        table_oid_(table_oid),
        lo_index_cols_(std::move(lo_index_cols)),
        hi_index_cols_(std::move(hi_index_cols)),
        scan_limit_(scan_limit)
        {}

  /**
   * Default constructor used for deserialization
   */
  IndexScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(IndexScanPlanNode)

  /**
   * @return the OID of the index
   */
  catalog::index_oid_t GetIndexOid() const { return index_oid_; }

  /**
   * @return the OID of the table
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetIndexColumns() const { return lo_index_cols_; }

  /**
   * @return the lower bound index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetLoIndexColumns() const { return lo_index_cols_; }

  /**
   * @return the upper bound index columns
   */
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetHiIndexColumns() const { return hi_index_cols_; }


  IndexScanType GetScanType() const {
    return scan_type_;
  }

  uint32_t ScanLimit() const {
    return scan_limit_;
  }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INDEXSCAN; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

  /**
   * Collect all column oids in this expression
   * @return the vector of unique columns oids
   */
  std::vector<catalog::col_oid_t> CollectInputOids() const {
    std::vector<catalog::col_oid_t> result;
    // Output expressions
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      CollectOids(&result, col.GetExpr());
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

  IndexScanType scan_type_;
  catalog::index_oid_t index_oid_;
  catalog::table_oid_t table_oid_;
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
  uint32_t scan_limit_;
};

DEFINE_JSON_DECLARATIONS(IndexScanPlanNode)

}  // namespace terrier::planner
