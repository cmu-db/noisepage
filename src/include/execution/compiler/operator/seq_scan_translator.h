#pragma once

#include <string_view>
#include <vector>

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"

namespace noisepage::catalog {
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::parser {
class AbstractExpression;
}  // namespace noisepage::parser

namespace noisepage::planner {
class SeqScanPlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

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

  void InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;
  void RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Generate the scan.
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Tear-down the FilterManager if required.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * @return The pipeline work function parameters. Just the *TVI.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * Launch a parallel table scan.
   * @param function The pipeline generating function.
   * @param work_func The worker function that'll be called during the parallel scan.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override;

  /**
   * @return The value (or value vector) of the column with the provided column OID in the table
   *         this sequential scan is operating over.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  ast::Expr *GetSlotAddress() const override;

  /** @return The expression representing the current VPI. */
  ast::Expr *GetVPI() const;

 private:
  // Does the scan have a predicate?
  bool HasPredicate() const;

  // Get the OID of the table being scanned.
  catalog::table_oid_t GetTableOid() const;

  // Set col_oids_var_ to contain the column OIDs that are being scanned over.
  void DeclareColOids(FunctionBuilder *function) const;

  // Generate a generic filter term.
  void GenerateGenericTerm(FunctionBuilder *function, common::ManagedPointer<parser::AbstractExpression> term,
                           ast::Expr *vector_proj, ast::Expr *tid_list);

  // Generate all filter clauses.
  void GenerateFilterClauseFunctions(util::RegionVector<ast::FunctionDecl *> *decls,
                                     common::ManagedPointer<parser::AbstractExpression> predicate,
                                     std::vector<ast::Identifier> *curr_clause, bool seen_conjunction);

  // Perform a table scan using the provided table vector iterator pointer.
  void ScanTable(WorkContext *ctx, FunctionBuilder *function) const;

  // Generate a scan over the VPI.
  void ScanVPI(WorkContext *ctx, FunctionBuilder *function, ast::Expr *vpi) const;

 private:
  // When the plan's oid list is empty (like in "SELECT COUNT(*)"), then we just read the first column of the table.
  // Otherwise we just read the plan's oid list.
  // This is because the storage layer needs to read at least one column.
  // TODO(Amadou): Create a special code path for COUNT(*).
  // This requires a new table iterator that doesn't materialize tuples as well as a few builtins.
  static std::vector<catalog::col_oid_t> MakeInputOids(const catalog::Schema &schema,
                                                       const planner::SeqScanPlanNode &op);

  /** @return The index of the given column OID inside the col_oids that the plan is scanning over. */
  uint32_t GetColOidIndex(catalog::col_oid_t col_oid) const;

  // The name of the declared TVI and VPI.
  ast::Identifier tvi_var_;
  ast::Identifier vpi_var_;
  // The name of the col_oids that the plan wants to scan over.
  ast::Identifier col_oids_var_;

  ast::Identifier slot_var_;

  // Where the filter manager exists.
  StateDescriptor::Entry local_filter_manager_;

  // The list of filter manager clauses. Populated during helper function
  // definition, but only if there's a predicate.
  std::vector<std::vector<ast::Identifier>> filters_;

  // The version of col_oids that we use for translation. See MakeInputOids for justification.
  std::vector<catalog::col_oid_t> col_oids_;

  // The number of rows that are scanned.
  StateDescriptor::Entry num_scans_;
};

}  // namespace noisepage::execution::compiler
