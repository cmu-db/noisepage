#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "catalog/postgres/pg_statistic.h"
#include "execution/compiler/operator/aggregate_util.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/analyze_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Bottom translator for Analyze: aggregates values from table scan
 * TODO(khg): This is essentially a photocopy of StaticAggregateBottomTranslator.
 *   Originally, I tried to have it inherit from that class instead, but apparently that doesn't work.
 *   Investigate ways to reduce code duplication.
 */
class AnalyzeBottomTranslator : public OperatorTranslator {
 public:
  AnalyzeBottomTranslator() = delete;

  /**
   * Create a new AnalyzeBottomTranslator
   * @param op plan node to translate
   * @param codegen code generator
   * @return new translator
   */
  AnalyzeBottomTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen);

  // Declare aggregates and distinct hash tables.
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare the values struct and the payload of each distinct aggregate
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Initialize comparison functions for distinct aggregates.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Initialize the aggregates
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Free Distinct
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  // Pass Through
  void Produce(FunctionBuilder *builder) override { child_translator_->Produce(builder); };
  // Pass Through
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }
  void Consume(FunctionBuilder *builder) override;

  // Pass through to the child
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return child_translator_->GetOutput(attr_idx);
  }

  // Return the attribute at idx
  ast::Expr *GetOutput(uint32_t attr_idx) override {
    ast::Expr *exec_ctx_expr = codegen_->MakeExpr(codegen_->GetExecCtxVar());
    ast::Expr *agg = codegen_->GetStateMemberPtr(helper_.GetAggregate(attr_idx));
    return codegen_->BuiltinCall(ast::Builtin::AggResult, {exec_ctx_expr, agg});
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  /**
   * Makes a fake AggregatePlanNode that describes the aggregations we want to compute.
   *
   * TODO(khg): This is only used to pass to AggregateHelper, which is an ugly hack. However, the AggregateHelper
   *   only really uses the aggregate and group-by expressions from the AggregatePlanNode, so it could be refactored.
   *
   * @param op AnalyzePlanNode containing information about the table and columns to be analyzed
   * @return An AggregatePlanNode containing the necessary aggregations.
   */
  static std::unique_ptr<planner::AggregatePlanNode> MakeAggregatePlanNode(
      const planner::AnalyzePlanNode *op, std::vector<std::unique_ptr<parser::AbstractExpression>> *owned_exprs);

  friend class AnalyzeTopTranslator;
  const planner::AnalyzePlanNode *op_;

  /** Owned aggregation expressions, passed to AggregateHelper */
  std::vector<std::unique_ptr<parser::AbstractExpression>> owned_exprs_;

  /** Fake AggregatePlanNode, passed to AggregateHelper */
  const std::unique_ptr<planner::AggregatePlanNode> agg_plan_node_;

  /** Helper used to generate aggregate code */
  AggregateHelper helper_;
};

/**
 * Top translator for Analyze: updates aggregated values in statistics table
 * This is heavily patterned off of UpdateTranslator.
 */
class AnalyzeTopTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node
   * @param codegen The code generator
   * @param bottom The corresponding bottom translator
   */
  AnalyzeTopTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen, OperatorTranslator *bottom);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    TERRIER_ASSERT(child_idx == 0, "AnalyzeTopTranslator should have only one child");
    return child_translator_->GetOutput(attr_idx);
  }

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Analyze doesn't output anything"); }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // Declare the updater
  void DeclareUpdater(FunctionBuilder *builder);
  // Free the updater
  void GenUpdaterFree(FunctionBuilder *builder);
  // Set the oids variable
  void SetOids(FunctionBuilder *builder);
  // Declare the update PR
  void DeclareUpdatePR(FunctionBuilder *builder);
  // Get the pr to update
  void GetUpdatePR(FunctionBuilder *builder);
  // Fill the update PR from the child's output
  void FillPRFromChild(FunctionBuilder *builder);
  // Update on table.
  void GenTableUpdate(FunctionBuilder *builder);

 private:
  const planner::AnalyzePlanNode *op_;
  AnalyzeBottomTranslator *bottom_;
  const catalog::Schema &table_schema_;
  storage::ProjectionMap table_pm_;

  // Structs, functions and locals
  ast::Identifier updater_;
  ast::Identifier update_pr_;
  ast::Identifier col_oids_;

  // Table OID to update
  static constexpr catalog::table_oid_t UPDATE_TABLE_OID = catalog::postgres::STATISTIC_TABLE_OID;

  // Column OIDs to update
  static constexpr std::array<catalog::col_oid_t, 4> UPDATE_COL_OIDS{
      catalog::postgres::STANULLFRAC_COL_OID, catalog::postgres::STADISTINCT_COL_OID,
      catalog::postgres::STA_NUMROWS_COL_OID, catalog::postgres::STA_TOPKELTS_COL_OID};
};

}  // namespace terrier::execution::compiler
