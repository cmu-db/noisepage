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
class OutputSchema;
}

namespace transaction {
class TransactionContext;
}

namespace optimizer {
class PropertySet;
class OperatorExpression;
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
  /**
   * Constructor
   */
  PlanGenerator();

  /**
   * Converts an operator expression into a plan node.
   *
   * @param op OperatorExpression to convert
   * @param required_props Required properties
   * @param required_cols Columns that are required to be output
   * @param output_cols Columns output by the Operator
   * @param children_plans Children plan nodes
   * @param children_expr_map Vector of children expression -> col offset mapping
   * @param estimated_cardinality Estimated cardinality
   * @param settings SettingsManager
   * @param accessor CatalogAccessor
   * @param txn TransactionContext
   * @returns Output plan node
   */
  planner::AbstractPlanNode* ConvertOpExpression(
      OperatorExpression* op,
      PropertySet* required_props,
      const std::vector<const parser::AbstractExpression *> &required_cols,
      const std::vector<const parser::AbstractExpression *> &output_cols,
      std::vector<planner::AbstractPlanNode*> &&children_plans,
      std::vector<ExprMap> &&children_expr_map,
      int estimated_cardinality,
      settings::SettingsManager *settings,
      catalog::CatalogAccessor *accessor,
      transaction::TransactionContext *txn);

  /**
   * Visitor function for a TableFreeScan operator
   * @param op TableFreeScan operator being visited
   */
  void Visit(const TableFreeScan *op) override;

  /**
   * Visitor function for a SeqScan operator
   * @param op SeqScan operator being visited
   */
  void Visit(const SeqScan *op) override;

  /**
   * Visitor function for a IndexScan operator
   * @param op IndexScan operator being visited
   */
  void Visit(const IndexScan *op) override;

  /**
   * Visitor function for a ExternalFileScan operator
   * @param op ExternalFileScan operator being visited
   */
  void Visit(const ExternalFileScan *op) override;

  /**
   * Visitor function for a QueryDerivedScan operator
   * @param op QueryDerivedScan operator being visited
   */
  void Visit(const QueryDerivedScan *op) override;

  /**
   * Visitor function for a OrdreBy operator
   * @param op OrderBy operator being visited
   */
  void Visit(const OrderBy *op) override;

  /**
   * Visitor function for a Limit operator
   * @param op Limit operator being visited
   */
  void Visit(const Limit *op) override;

  /**
   * Visitor function for a InnerNLJoin operator
   * @param op InnerNLJoin operator being visited
   */
  void Visit(const InnerNLJoin *op) override;

  /**
   * Visitor function for a LeftNLJoin operator
   * @param op LeftNLJoin operator being visited
   */
  void Visit(const LeftNLJoin *op) override;

  /**
   * Visitor function for a RightNLJoin operator
   * @param op RightNLJoin operator being visited
   */
  void Visit(const RightNLJoin *op) override;

  /**
   * Visitor function for a OuterNLJoin operator
   * @param op OuterNLJoin operator being visited
   */
  void Visit(const OuterNLJoin *op) override;

  /**
   * Visitor function for a InnerHashJoin operator
   * @param op InnerHashJoin operator being visited
   */
  void Visit(const InnerHashJoin *op) override;

  /**
   * Visitor function for a LeftHashJoin operator
   * @param op LeftHashJoin operator being visited
   */
  void Visit(const LeftHashJoin *op) override;

  /**
   * Visitor function for a RightHashJoin operator
   * @param op RightHashJoin operator being visited
   */
  void Visit(const RightHashJoin *op) override;

  /**
   * Visitor function for a OuterHashJoin operator
   * @param op OuterHashJoin operator being visited
   */
  void Visit(const OuterHashJoin *op) override;

  /**
   * Visitor function for a Insert operator
   * @param op Insert operator being visited
   */
  void Visit(const Insert *op) override;

  /**
   * Visitor function for a InsertSelect operator
   * @param op InsertSelect operator being visited
   */
  void Visit(const InsertSelect *op) override;

  /**
   * Visitor function for a Delete operator
   * @param op Delete operator being visited
   */
  void Visit(const Delete *op) override;

  /**
   * Visitor function for a Update operator
   * @param op Update operator being visited
   */
  void Visit(const Update *op) override;

  /**
   * Visitor function for a HashGroupBy operator
   * @param op HashGroupBy operator being visited
   */
  void Visit(const HashGroupBy *op) override;

  /**
   * Visitor function for a SortGroupBy operator
   * @param op SortGroupBy operator being visited
   */
  void Visit(const SortGroupBy *op) override;

  /**
   * Visitor function for a Distinct operator
   * @param op Distinct operator being visited
   */
  void Visit(const Distinct *op) override;

  /**
   * Visitor function for a Aggregate operator
   * @param op Aggregate operator being visited
   */
  void Visit(const Aggregate *op) override;

  /**
   * Visitor function for a ExportExternalFile operator
   * @param op ExportExternalFile operator being visited
   */
  void Visit(const ExportExternalFile *op) override;

 private:
  /**
   * Register a pointer to be deleted on transaction commit/abort
   * @param ptr Pointer to delete
   * @param onCommit Whether to delete on transaction commit
   * @param onAbort Whether to delete on transaction abort
   */
  void RegisterPointerCleanup(void *ptr, bool onCommit, bool onAbort);

  /**
   * Generate all tuple value expressions of a base table
   *
   * @param alias Table alias used in constructing ColumnValue
   * @param db_oid Database OID
   * @param tbl_oid Table OID for catalog lookup
   *
   * @return a vector of tuple value expression representing column name to
   * table column id mapping
   */
  std::vector<const parser::AbstractExpression*>
  GenerateTableColumnValueExprs(
    const std::string &alias,
    catalog::database_oid_t db_oid,
    catalog::table_oid_t tbl_oid);

  /**
   * Generate the column oids vector for a scan plan
   *
   * @return a vector of column oid indicating which columns to scan
   */
  std::vector<catalog::col_oid_t> GenerateColumnsForScan();

  /**
   * Generate a predicate expression for scan plans
   *
   * @param predicate_expr the original expression
   * @param alias the table alias
   * @param db_oid Database OID
   * @param tbl_oid Table OID for catalog lookup
   *
   * @return a predicate that is already evaluated, which could be used to
   * generate a scan plan i.e. all tuple idx are set
   */
  const parser::AbstractExpression* GeneratePredicateForScan(
      const parser::AbstractExpression* predicate_expr,
      const std::string &alias,
      catalog::database_oid_t, db_oid,
      catalog::table_oid_t tbl_oid);

  /**
   * Generate projection info and projection schema for join
   *
   * @param proj_info Projection OutputSchema
   * @param proj_schema The projection schema object
   */
  void GenerateProjectionForJoin(
      planner::OutputSchema *proj_info,
      const catalog::Schema &proj_schema);

  /**
   * Check required columns and output_cols, see if we need to add
   * projection on top of the current output plan, this should be done after
   * the output plan produciing output columns is generated
   */
  void BuildProjectionPlan();

  /**
   * Constructs an Aggregate Plan
   * @param aggr_type AggregateType
   * @param groupby_cols Vector of GroupBy expressions
   * @param having Having clause expression
   */
  void BuildAggregatePlan(
      AggregateType aggr_type,
      const std::vector<const parser::AbstractExpression> *groupby_cols,
      const parser::AbstractExpression* having);

  /**
   * The required output property. Note that we have previously enforced
   * properties so this is fulfilled by the current operator
   */
  PropertySet* required_props_;

  /**
   * Required columns, this may not be fulfilled by the operator, but we
   * can always generate a projection if the output column does not fulfill the
   * requirement
   */
  const std::vector<const parser::AbstractExpression *> required_cols_;

  /**
   * The output columns, which can be fulfilled by the current operator.
   */
  const std::vector<const parser::AbstractExpression *> output_cols_;

  /**
   * Vector of child plans
   */
  std::vector<planner::AbstractPlanNode *> children_plans_;

  /**
   * The expression maps (expression -> tuple idx)
   */
  std::vector<ExprMap> children_expr_map_;

  /**
   * Final output plan
   */
  planner::AbstractPlanNode* output_plan_;

  /**
   * Settings Manager
   */
  settings::SettingsManager* settings_;

  /**
   * CatalogAccessor
   */
  catalog::CatalogAccessor* accessor_;

  /**
   * Transaction Context executing under
   */
  transaction::TransactionContext *txn_;
};

}  // namespace optimizer
}  // namespace terrier
