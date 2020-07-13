#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"

namespace terrier::planner {
class CSVScanPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

class FunctionBuilder;

/** Translates CSV scan plans. */
class CSVScanTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given scan plan.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  CSVScanTranslator(const planner::CSVScanPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Define the base row type this scan produces.
   * @param decls The top-level declaration list.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Generate the CSV scan logic.
   * @param context The context of work.
   * @param function The function being built.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * CSV Scans are always serial, so should never launch work.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override {
    UNREACHABLE("CSV scans are always serial ... for now.");
  }

  /**
   * CSV Scans are always serial, so should never launch work.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("CSV scans are always serial ... for now.");
  }

  /**
   * Access a column from the base CSV.
   * @param col_oid The ID of the column to read.
   * @return The value of the column.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:
  // Return the plan.
  const planner::CSVScanPlanNode &GetCSVPlan() const { return GetPlanAs<planner::CSVScanPlanNode>(); }

  // Access the given field in the CSV row.
  ast::Expr *GetField(uint32_t field_index) const;
  // Access a pointer to the field in the CSV row.
  ast::Expr *GetFieldPtr(uint32_t field_index) const;

 private:
  // The name of the base row variable.
  ast::Identifier base_row_type_;
  StateDescriptor::Entry base_row_;
};

}  // namespace terrier::execution::compiler
