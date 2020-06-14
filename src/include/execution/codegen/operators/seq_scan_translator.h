#pragma once

#include <string_view>
#include <vector>

#include "execution/sql/codegen/ast_fwd.h"
#include "execution/sql/codegen/operators/operator_translator.h"
#include "execution/sql/codegen/pipeline.h"
#include "execution/sql/codegen/pipeline_driver.h"
#include "execution/sql/schema.h"

namespace terrier::execution::sql::planner {
class AbstractExpression;
class SeqScanPlanNode;
}  // namespace terrier::execution::sql::planner

namespace terrier::execution::codegen {

class FunctionBuilder;

/**
 * A translator for sequential table scans.
 */
class SeqScanTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  SeqScanTranslator(const planner::SeqScanPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(SeqScanTranslator);

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override;

  /**
   * Initialize the FilterManager if required.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Generate the scan.
   * @param context The context of the work.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Tear-down the FilterManager if required.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override;

  /**
   * @return The pipeline work function parameters. Just the *TVI.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * Launch a parallel table scan.
   * @param work_func The worker function that'll be called during the parallel scan.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override;

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override;

 private:
  // Does the scan have a predicate?
  bool HasPredicate() const;

  // Get the name of the table being scanned.
  std::string_view GetTableName() const;

  // Generate a generic filter term.
  void GenerateGenericTerm(FunctionBuilder *function, const planner::AbstractExpression *term, ast::Expr *vector_proj,
                           ast::Expr *tid_list);

  // Generate all filter clauses.
  void GenerateFilterClauseFunctions(util::RegionVector<ast::FunctionDecl *> *decls,
                                     const planner::AbstractExpression *predicate,
                                     std::vector<ast::Identifier> *curr_clause, bool seen_conjunction);

  // Perform a table scan using the provided table vector iterator pointer.
  void ScanTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate a scan over the VPI.
  void ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const;

 private:
  // The name of the declared TVI and VPI.
  ast::Identifier tvi_var_;
  ast::Identifier vpi_var_;

  // Where the filter manager exists.
  StateDescriptor::Entry local_filter_manager_;

  // The list of filter manager clauses. Populated during helper function
  // definition, but only if there's a predicate.
  std::vector<std::vector<ast::Identifier>> filters_;
};

}  // namespace terrier::execution::codegen
