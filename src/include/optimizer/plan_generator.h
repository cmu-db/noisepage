#pragma once

#include "catalog/schema.h"
#include "optimizer/operator_visitor.h"

namespace terrier {

namespace planner {
class AbstractPlanNode;
class HashJoinPlanNode;
class NestedLoopJoinPlanNode;
class ProjectionPlanNode;
class SeqScanPlanNode;
class AggregatePlanNode;

// TODO(wz2): Do we port this?
class ProjectInfo;
}

namespace transaction {
class TransactionContext;
}

namespace optimizer {
class PropertySet;
class OperatorExpression;
}

// TODO(wz2): Revisit this when there is a catalog
namespace catalog {
class TableCatalogEntry;
}

namespace optimizer {

enum class AggregateType {
  INVALID = -1,
  SORTED = 1,
  HASH = 2,
  PLAIN = 3  // no group-by
};

class PlanGenerator : public OperatorVisitor {
 public:
  PlanGenerator();

  planner::AbstractPlanNode* ConvertOpExpression(
      OperatorExpression* op,
      PropertySet* required_props,
      std::vector<const parser::AbstractExpression *> required_cols,
      std::vector<const parser::AbstractExpression *> output_cols,
      std::vector<planner::AbstractPlanNode*> &children_plans,
      std::vector<ExprMap> children_expr_map,
      int estimated_cardinality);

  void Visit(const TableFreeScan *) override;
  void Visit(const SeqScan *) override;
  void Visit(const IndexScan *) override;
  void Visit(const ExternalFileScan *) override;
  void Visit(const QueryDerivedScan *) override;
  void Visit(const OrderBy *) override;
  void Visit(const Limit *) override;
  void Visit(const InnerNLJoin *) override;
  void Visit(const LeftNLJoin *) override;
  void Visit(const RightNLJoin *) override;
  void Visit(const OuterNLJoin *) override;
  void Visit(const InnerHashJoin *) override;
  void Visit(const LeftHashJoin *) override;
  void Visit(const RightHashJoin *) override;
  void Visit(const OuterHashJoin *) override;
  void Visit(const Insert *) override;
  void Visit(const InsertSelect *) override;
  void Visit(const Delete *) override;
  void Visit(const Update *) override;
  void Visit(const HashGroupBy *) override;
  void Visit(const SortGroupBy *) override;
  void Visit(const Distinct *) override;
  void Visit(const Aggregate *) override;
  void Visit(const ExportExternalFile *) override;

 private:
  /**
   * @brief Generate all tuple value expressions of a base table
   *
   * @param alias Table alias, we used it to construct the tuple value
   *  expression
   * @param table The table object
   *
   * @return a vector of tuple value expression representing column name to
   *  table column id mapping
   */
  std::vector<const parser::AbstractExpression*>
  GenerateTableTVExprs(const std::string &alias, catalog::TableCatalogEntry *table);

  /**
   * @brief Generate the column oids vector for a scan plan
   *
   * @return a vector of column oid indicating which columns to scan
   */
  std::vector<catalog::col_oid_t> GenerateColumnsForScan();

  /**
   * @brief Generate a predicate expression for scan plans
   *
   * @param predicate_expr the original expression
   * @param alias the table alias
   * @param table the table object
   *
   * @return a predicate that is already evaluated, which could be used to
   *  generate a scan plan i.e. all tuple idx are set
   */
  const parser::AbstractExpression* GeneratePredicateForScan(
      const parser::AbstractExpression* predicate_expr,
      const std::string &alias,
      catalog::TableCatalogEntry* table);

  /**
   * @brief Generate projection info and projection schema for join
   *
   * @param proj_info The projection info object
   * @param proj_schema The projection schema object
   */
  void GenerateProjectionForJoin(
      std::unique_ptr<const planner::ProjectInfo> &proj_info,
      const catalog::Schema &proj_schema);

  /**
   * @brief Check required columns and output_cols, see if we need to add
   *  projection on top of the current output plan, this should be done after
   *  the output plan produciing output columns is generated
   */
  void BuildProjectionPlan();
  void BuildAggregatePlan(
      AggregateType aggr_type,
      const std::vector<const parser::AbstractExpression> *groupby_cols,
      const parser::AbstractExpression* having);

  /**
   * @brief The required output property. Note that we have previously enforced
   *  properties so this is fulfilled by the current operator
   */
  PropertySet* required_props_;

  /**
   * @brief Required columns, this may not be fulfilled by the operator, but we
   *  can always generate a projection if the output column does not fulfill the
   *  requirement
   */
  std::vector<const parser::AbstractExpression *> required_cols_;

  /**
   * @brief The output columns, which can be fulfilled by the current operator.
   */
  std::vector<const parser::AbstractExpression *> output_cols_;
  std::vector<planner::AbstractPlanNode *> children_plans_;

  /**
   * @brief The expression maps (expression -> tuple idx)
   */
  std::vector<ExprMap> children_expr_map_;

  /**
   * @brief The final output plan
   */
  planner::AbstractPlanNode* output_plan_;

  transaction::TransactionContext *txn_;
};

}  // namespace optimizer
}  // namespace terrier
