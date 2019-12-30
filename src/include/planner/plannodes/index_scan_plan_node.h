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
#include "type/transient_value.h"

// TODO(Gus,Wen): plan node contained info on whether the scan was left or right open. This should be computed at
// execution time

namespace terrier::planner {

/**
 * Index Scan Predicate Description
 */
class IndexScanDescription {
 public:
  /**
   * Default constructor for JSON
   */
  IndexScanDescription() = default;

  /**
   * Constructor
   * @param tuple_oids Tuple Column OIDs with predicates
   * @param expr_list ExpressionType list of comparisons
   * @param val_list Value list of comparisons (bound + parameters)
   */
  IndexScanDescription(std::vector<catalog::col_oid_t> &&tuple_oids, std::vector<parser::ExpressionType> &&expr_list,
                       std::vector<type::TransientValue> &&val_list)
      : tuple_column_oid_list_(tuple_oids), expr_list_(expr_list), value_list_(std::move(val_list)) {}

  /**
   * Move constructor
   * @param other Other to move from
   */
  IndexScanDescription(IndexScanDescription &&other) noexcept
      : tuple_column_oid_list_(std::move(other.tuple_column_oid_list_)),
        expr_list_(std::move(other.expr_list_)),
        value_list_(std::move(other.value_list_)) {}

  /**
   * Move assignment
   * @param other Other to move from
   */
  IndexScanDescription &operator=(IndexScanDescription &&other) noexcept {
    tuple_column_oid_list_ = std::move(other.tuple_column_oid_list_);
    expr_list_ = std::move(other.expr_list_);
    value_list_ = std::move(other.value_list_);
    return *this;
  }

  /**
   * Checks equality against other IndexScanDescription
   * @param other IndexScanDescription to check against
   * @returns true if equal
   */
  bool operator==(const IndexScanDescription &other) const {
    if (tuple_column_oid_list_ != other.tuple_column_oid_list_) return false;
    if (expr_list_ != other.expr_list_) return false;
    return value_list_ == other.value_list_;
  }

  /**
   * Checks in-equality against other IndexScanDescription
   * @param other IndexScanDescription to check against
   * @returns true if not equal
   */
  bool operator!=(const IndexScanDescription &other) const { return !(*this == other); }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(tuple_column_oid_list_);
    hash = common::HashUtil::CombineHashInRange(hash, expr_list_.begin(), expr_list_.end());
    hash = common::HashUtil::CombineHashInRange(hash, value_list_.begin(), value_list_.end());
    return hash;
  }

  /**
   * Serializes this as json
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["tuple_column_oid_list"] = tuple_column_oid_list_;
    j["expr_list"] = expr_list_;
    j["value_list"] = value_list_;
    return j;
  }

  /**
   * Deserializes from JSON
   * @param j json
   */
  void FromJson(const nlohmann::json &j) {
    tuple_column_oid_list_ = j.at("tuple_column_oid_list").get<std::vector<catalog::col_oid_t>>();
    expr_list_ = j.at("expr_list").get<std::vector<parser::ExpressionType>>();
    value_list_ = j.at("value_list").get<std::vector<type::TransientValue>>();
  }

  /**
   * @returns column oids in base table that have scan predicate
   */
  const std::vector<catalog::col_oid_t> &GetTupleColumnIdList() const { return tuple_column_oid_list_; }

  /**
   * @returns comparison expression type list
   */
  const std::vector<parser::ExpressionType> &GetExpressionTypeList() const { return expr_list_; }

  /**
   * @returns comparison values
   */
  const std::vector<type::TransientValue> &GetValueList() const { return value_list_; }

 private:
  /**
   * Subset of column oids referenced in the scan predicate that should be used
   * for probing into the relevant index.
   */
  std::vector<catalog::col_oid_t> tuple_column_oid_list_;

  /**
   * List of comparison expression types
   */
  std::vector<parser::ExpressionType> expr_list_;

  /**
   * List of values
   */
  std::vector<type::TransientValue> value_list_;
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
     * Sets the scan limit
     */
    Builder &SetScanLimit(uint32_t limit) {
      scan_limit_ = limit;
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
     * @param desc IndexScanDescription for index scan
     * @return builder object
     */
    Builder &SetIndexScanDescription(IndexScanDescription &&desc) {
      index_scan_desc_ = std::move(desc);
      return *this;
    }

    /**
     * Build the Index scan plan node
     * @return plan node
     */
    std::unique_ptr<IndexScanPlanNode> Build() {
      return std::unique_ptr<IndexScanPlanNode>(new IndexScanPlanNode(
          std::move(children_), std::move(output_schema_), scan_predicate_, std::move(column_oids_),
          std::move(index_scan_desc_), is_for_update_, database_oid_, namespace_oid_, index_oid_, table_oid_,
          scan_type_, std::move(lo_index_cols_), std::move(hi_index_cols_), scan_limit_));
    }

   private:
    IndexScanType scan_type_;
    catalog::index_oid_t index_oid_;
    catalog::table_oid_t table_oid_;
    std::vector<catalog::col_oid_t> column_oids_;
    IndexScanDescription index_scan_desc_;
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
    uint32_t scan_limit_{0};
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param column_oids OIDs of columns to scan
   * @param scan_desc IndexScanDescription for the index scan
   * @param is_for_update scan is used for an update
   * @param database_oid database oid for scan
   * @param index_oid OID of index to be used in index scan
   * @param table_oid OID of the table
   * @param scan_type Type of the scan
   * @param lo_index_cols lower bound of the scan (or exact key when scan type = Exact).
   * @param hi_index_cols upper bound of the scan
   * @param scan_limit limit of the scan if any
   */
  IndexScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema,
                    common::ManagedPointer<parser::AbstractExpression> predicate,
                    std::vector<catalog::col_oid_t> &&column_oids, IndexScanDescription &&scan_desc, bool is_for_update,
                    catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid,
                    catalog::index_oid_t index_oid, catalog::table_oid_t table_oid, IndexScanType scan_type,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&lo_index_cols,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&hi_index_cols,
                    uint32_t scan_limit)
      : AbstractScanPlanNode(std::move(children), std::move(output_schema), predicate, is_for_update, database_oid,
                             namespace_oid),
        scan_type_(scan_type),
        index_oid_(index_oid),
        table_oid_(table_oid),
        column_oids_(column_oids),
        index_scan_desc_(std::move(scan_desc)),
        lo_index_cols_(std::move(lo_index_cols)),
        hi_index_cols_(std::move(hi_index_cols)),
        scan_limit_(scan_limit) {}

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
   * @return OIDs of columns to scan
   */
  const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_oids_; }

  /**
   * @returns Index Scan Description
   */
  const IndexScanDescription &GetIndexScanDescription() const { return index_scan_desc_; }

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
   * @return The scan type
   */
  IndexScanType GetScanType() const { return scan_type_; }

  /**
   * @return The scan limit
   */
  uint32_t ScanLimit() const { return scan_limit_; }

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
   * @warn slow!
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
  void CollectOids(std::vector<catalog::col_oid_t> *result,
                   common::ManagedPointer<parser::AbstractExpression> expr) const {
    if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto column_val = expr.CastManagedPointerTo<parser::ColumnValueExpression>();
      result->emplace_back(column_val->GetColumnOid());
    } else {
      for (const auto &child : expr->GetChildren()) {
        CollectOids(result, child);
      }
    }
  }

  IndexScanType scan_type_;
  catalog::index_oid_t index_oid_;
  catalog::table_oid_t table_oid_;
  std::vector<catalog::col_oid_t> column_oids_;
  IndexScanDescription index_scan_desc_;
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> lo_index_cols_{};
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> hi_index_cols_{};
  uint32_t scan_limit_;
};

DEFINE_JSON_DECLARATIONS(IndexScanDescription)
DEFINE_JSON_DECLARATIONS(IndexScanPlanNode)

}  // namespace terrier::planner
