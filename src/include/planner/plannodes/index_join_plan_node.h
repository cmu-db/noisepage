#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
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
    Builder() = default;

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
     * TODO(Amadou): Ideally, these expressions should come from the catalog.
     * But the optimizer may change the expression, so perhaps this is the right place
     */
    Builder &AddIndexColum(IndexExpression expr) {
      index_cols_.emplace_back(expr);
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

    /**
     * Index Cols
     */
    std::vector<IndexExpression> index_cols_{};
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
                    catalog::table_oid_t table_oid, std::vector<IndexExpression> &&index_cols)
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
  const std::vector<IndexExpression> &GetIndexColumns() const { return index_cols_; }

 private:
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
  std::vector<IndexExpression> index_cols_{};
};

DEFINE_JSON_DECLARATIONS(IndexJoinPlanNode);

}  // namespace terrier::planner
