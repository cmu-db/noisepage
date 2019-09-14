#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

using IndexExpression = std::shared_ptr<parser::AbstractExpression>;

/**
 * Plan node for nested loop joins
 */
class IndexJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for nested loop join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder () = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::shared_ptr<IndexJoinPlanNode> Build() {
      return std::shared_ptr<IndexJoinPlanNode>(new IndexJoinPlanNode(std::move(children_), std::move(output_schema_),
                                                                      join_type_, std::move(join_predicate_),
                                                                      index_oid_, table_oid_, std::move(index_cols_)));
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
      index_cols_.emplace(col_oid, expr);
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

    /**line:0:0>
|     | |- IndexExpr (0x629000001180) <line:0:0>  'uint32'
|     | | |- IdentifierExpr (0x629000001150) <line:0:0>  '[2
     * Index Cols
     */
    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> index_cols_{};
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  IndexJoinPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                    std::shared_ptr<parser::AbstractExpression> predicate, catalog::index_oid_t index_oid,
                    catalog::table_oid_t table_oid,
                    std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &&index_cols)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, std::move(predicate)),
        index_oid_(index_oid),
        table_oid_(table_oid),
        index_cols_(std::move(index_cols)) {}

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
   * @return the NestedLooped value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

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
  const std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> &GetIndexColumns() const { return index_cols_; }

  /**
   * Collect all column oids in this expression
   * @return the vector of unique columns oids
   */
  std::vector<catalog::col_oid_t> CollectInputOids() const {
    std::vector<catalog::col_oid_t> result;
    // Scan predicate
    if (GetJoinPredicate() != nullptr) CollectOids(&result, GetJoinPredicate().get());
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
        CollectOids(result, child.get());
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
  /**
   * Index columns
   */
  std::unordered_map<catalog::indexkeycol_oid_t, IndexExpression> index_cols_{};
};

DEFINE_JSON_DECLARATIONS(IndexJoinPlanNode);

}  // namespace terrier::planner
