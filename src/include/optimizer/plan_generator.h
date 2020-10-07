#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/operator_visitor.h"
#include "transaction/transaction_context.h"

namespace terrier {

namespace planner {
class AbstractPlanNode;
class HashJoinPlanNode;
class NestedLoopJoinPlanNode;
class ProjectionPlanNode;
class SeqScanPlanNode;
class AggregatePlanNode;
class OutputSchema;
}  // namespace planner

namespace settings {
class SettingsManager;
}  // namespace settings

namespace catalog {
class CatalogAccessor;
class Schema;
}  // namespace catalog

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace planner {
enum class AggregateStrategyType;
}

namespace optimizer {

class PropertySet;
class OperatorNode;

/**
 * Plan Generator for generating plans from Operators
 */
class PlanGenerator : public OperatorVisitor {
 public:
  /**
   * Constructor
   */
  PlanGenerator();

  /**
   * Converts an operator node into a plan node.
   *
   * @param txn TransactionContext
   * @param accessor CatalogAccessor
   * @param op OperatorNode to convert
   * @param required_props Required properties
   * @param required_cols Columns that are required to be output
   * @param output_cols Columns output by the Operator
   * @param children_plans Children plan nodes
   * @param children_expr_map Vector of children expression -> col offset mapping
   * @returns Output plan node
   */
  std::unique_ptr<planner::AbstractPlanNode> ConvertOpNode(
      transaction::TransactionContext *txn, catalog::CatalogAccessor *accessor, AbstractOptimizerNode *op,
      PropertySet *required_props, const std::vector<common::ManagedPointer<parser::AbstractExpression>> &required_cols,
      const std::vector<common::ManagedPointer<parser::AbstractExpression>> &output_cols,
      std::vector<std::unique_ptr<planner::AbstractPlanNode>> &&children_plans,
      std::vector<ExprMap> &&children_expr_map);

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
   * Visitor function for a InnerIndexJoin operator
   * @param op InnerIndexJoin operator being visited
   */
  void Visit(const InnerIndexJoin *op) override;

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
   * Visitor function for a LeftSemiHashJoin operator
   * @param op LeftSemiHashJoin operator being visited
   */
  void Visit(const LeftSemiHashJoin *op) override;

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
   * Visitor function for a Aggregate operator
   * @param op Aggregate operator being visited
   */
  void Visit(const Aggregate *op) override;

  /**
   * Visitor function for a ExportExternalFile operator
   * @param op ExportExternalFile operator being visited
   */
  void Visit(const ExportExternalFile *op) override;

  /**
   * Visit a CreateDatabase operator
   * @param create_database operator
   */
  void Visit(const CreateDatabase *create_database) override;

  /**
   * Visit a CreateFunction operator
   * @param create_function operator
   */
  void Visit(const CreateFunction *create_function) override;

  /**
   * Visit a CreateIndex operator
   * @param create_index operator
   */
  void Visit(const CreateIndex *create_index) override;

  /**
   * Visit a CreateTable operator
   * @param create_table operator
   */
  void Visit(const CreateTable *create_table) override;

  /**
   * Visit a CreateNamespace operator
   * @param create_namespace operator
   */
  void Visit(const CreateNamespace *create_namespace) override;

  /**
   * Visit a CreateTrigger operator
   * @param create_trigger operator
   */
  void Visit(const CreateTrigger *create_trigger) override;

  /**
   * Visit a CreateView operator
   * @param create_view operator
   */
  void Visit(const CreateView *create_view) override;
  /**
   * Visit a DropDatabase operator
   * @param drop_database operator
   */
  void Visit(const DropDatabase *drop_database) override;

  /**
   * Visit a DropTable operator
   * @param drop_table operator
   */
  void Visit(const DropTable *drop_table) override;

  /**
   * Visit a DropIndex operator
   * @param drop_index operator
   */
  void Visit(const DropIndex *drop_index) override;

  /**
   * Visit a DropNamespace operator
   * @param drop_namespace operator
   */
  void Visit(const DropNamespace *drop_namespace) override;

  /**
   * Visit a DropTrigger operator
   * @param drop_trigger operator
   */
  void Visit(const DropTrigger *drop_trigger) override;

  /**
   * Visit a DropView operator
   * @param drop_view operator
   */
  void Visit(const DropView *drop_view) override;

  /**
   * Visit a Analyze operator
   * @param analyze operator
   */
  void Visit(const Analyze *analyze) override;

 private:
  /**
   * Register a pointer to be deleted on transaction commit/abort
   * @param ptr Pointer to delete
   * @param onCommit Whether to delete on transaction commit
   * @param onAbort Whether to delete on transaction abort
   */
  template <class T>
  void RegisterPointerCleanup(void *ptr, bool onCommit, bool onAbort) {
    if (onCommit) {
      txn_->RegisterCommitAction([=]() { delete reinterpret_cast<T *>(ptr); });
    }

    if (onAbort) {
      txn_->RegisterAbortAction([=]() { delete reinterpret_cast<T *>(ptr); });
    }
  }

  /**
   * Generate the column oids vector for a scan plan
   * @param predicate Predicate of the scan
   * @return a vector of column oid indicating which columns to scan
   */
  std::vector<catalog::col_oid_t> GenerateColumnsForScan(const parser::AbstractExpression *predicate);

  /**
   * Read the oids contained in an expression.
   * @param oids Oids contained in the given expression.
   * @param expr Expression to read.
   */
  void GenerateColumnsFromExpression(std::unordered_set<catalog::col_oid_t> *oids,
                                     const parser::AbstractExpression *expr);

  /**
   * Generates the OutputSchema for a scan.
   * The OutputSchema contains only those columns in output_cols_
   * @param tbl_oid Table OID of table being scanned
   * @returns OutputSchema
   */
  std::unique_ptr<planner::OutputSchema> GenerateScanOutputSchema(catalog::table_oid_t tbl_oid);

  /**
   * Generate projection info and projection schema for join
   * @returns output schema of projection
   */
  std::unique_ptr<planner::OutputSchema> GenerateProjectionForJoin();

  /**
   * The Plan node's OutputSchema may not match the required columns. As such,
   * this function adds a projection on top of the output plan which will ensure
   * that the correct output columns is generated. This is done by adding a
   * ProjectionPlan.
   */
  void CorrectOutputPlanWithProjection();

  /**
   * Constructs an Aggregate Plan
   * @param aggr_type AggregateType
   * @param groupby_cols Vector of GroupBy expressions
   * @param having_predicate Having clause expression
   */
  void BuildAggregatePlan(planner::AggregateStrategyType aggr_type,
                          const std::vector<common::ManagedPointer<parser::AbstractExpression>> *groupby_cols,
                          common::ManagedPointer<parser::AbstractExpression> having_predicate);

  /**
   * The required output property. Note that we have previously enforced
   * properties so this is fulfilled by the current operator
   */
  PropertySet *required_props_;

  /**
   * Required columns, this may not be fulfilled by the operator, but we
   * can always generate a projection if the output column does not fulfill the
   * requirement
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols_;

  /**
   * The output columns, which can be fulfilled by the current operator.
   */
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols_;

  /**
   * Vector of child plans
   */
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> children_plans_;

  /**
   * The expression maps (expression -> tuple idx)
   */
  std::vector<ExprMap> children_expr_map_;

  /**
   * Final output plan
   */
  std::unique_ptr<planner::AbstractPlanNode> output_plan_;

  /**
   * CatalogAccessor
   */
  catalog::CatalogAccessor *accessor_;

  /**
   * Transaction Context executing under
   */
  transaction::TransactionContext *txn_;
};

}  // namespace optimizer
}  // namespace terrier
