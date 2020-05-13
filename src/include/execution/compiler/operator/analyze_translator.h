#pragma once

#include <utility>
#include <vector>

#include "catalog/postgres/pg_statistic.h"
#include "execution/compiler/operator/aggregate_util.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/operator/static_aggregate_translator.h"
#include "planner/plannodes/analyze_plan_node.h"

namespace terrier::execution::compiler {

class AnalyzeBottomTranslator : public StaticAggregateBottomTranslator {
 public:
  AnalyzeBottomTranslator() = delete;

  AnalyzeBottomTranslator(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen,
                          std::unique_ptr<planner::AggregatePlanNode> &&agg_plan_node,
                          std::vector<std::unique_ptr<parser::AbstractExpression>> &&owned_exprs);

  /**
   * Create a new AnalyzeBottomTranslator
   * TODO(khg): This hack is only necessary due to the MakeAggregatePlanNode hack.
   * @param op plan node to translate
   * @param codegen code generator
   * @return new translator
   */
  static std::unique_ptr<AnalyzeBottomTranslator> Create(const terrier::planner::AnalyzePlanNode *op, CodeGen *codegen);

 private:
  /**
   * Makes a fake AggregatePlanNode that describes the aggregations we want to compute.
   * This is only to make the AggregateHelper happy. Note that it only uses the GetAggregateTerms() and
   * the GetGroupByTerms() methods on the plan node passed in.
   *
   * @param op AnalyzePlanNode containing information about the table and columns to be analyzed
   * @return An AggregatePlanNode containing the necessary aggregations.
   */
  static std::unique_ptr<planner::AggregatePlanNode> MakeAggregatePlanNode(
      const planner::AnalyzePlanNode *op, std::vector<std::unique_ptr<parser::AbstractExpression>> *owned_exprs);

  /** Original Analyze plan node */
  const planner::AnalyzePlanNode *analyze_plan_node_;

  /** Fake Analyze plan node, passed to StaticAggregateBottomTranslator */
  const std::unique_ptr<planner::AggregatePlanNode> agg_plan_node_;

  /** Owned aggregation expressions, passed to AggregateHelper */
  std::vector<std::unique_ptr<parser::AbstractExpression>> owned_exprs_;
};

/**
 * Analyze Translator
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
  static constexpr std::array<catalog::col_oid_t, 3> UPDATE_COL_OIDS{catalog::postgres::STANULLFRAC_COL_OID,
                                                                     catalog::postgres::STADISTINCT_COL_OID,
                                                                     catalog::postgres::STA_NUMROWS_COL_OID};
};

}  // namespace terrier::execution::compiler
