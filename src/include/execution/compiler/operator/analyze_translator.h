#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "storage/storage_defs.h"

namespace noisepage::execution::compiler {

/**
 * AnalyzeTranslator
 */
class AnalyzeTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create new translator for the given analyze plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The analyze plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline pipeline The pipeline this operator is participating in.
   */
  AnalyzeTranslator(const planner::AnalyzePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Implement analyze logic where it uses the aggregate values of it's child to fill the pg_statistic table.
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return The list of extra fields added to the "work" function. By default, the first two
   *         arguments are the query state and the pipeline state.
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;

  /**
   * This is called to launch the provided worker function in parallel across a set of threads.
   * @param function The function being built.
   * @param work_func_name The name of the work function that implements the pipeline logic.
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override;

  /**
   *
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:
  catalog::Schema pg_statistic_table_schema_;
  storage::ProjectionMap pg_statistic_table_pm_;
  catalog::IndexSchema pg_statistic_index_schema_;
  std::unordered_map<catalog::indexkeycol_oid_t, uint16_t> pg_statistic_index_pm_;
  ast::Identifier pg_statistic_col_oids_;
  ast::Identifier table_oid_;
  ast::Identifier col_oid_;
  ast::Identifier num_rows_;
  std::vector<ast::Identifier> aggregate_variables_;
  // Maps a column oid to the variable that holds the value to insert into that column
  std::unordered_map<catalog::col_oid_t, ast::Identifier> pg_statistic_column_lookup_;
  ast::Identifier pg_statistic_index_iterator_;
  ast::Identifier pg_statistic_index_pr_;
  ast::Identifier pg_statistic_updater_;
  ast::Identifier pg_statistic_update_pr_;

  void SetPgStatisticColOids(FunctionBuilder *function) const;
  void InitPgStatisticVariables(WorkContext *context, FunctionBuilder *function) const;
  void DeclarePgStatisticIterator(FunctionBuilder *function) const;
  void DeclarePgStatisticIndexPR(FunctionBuilder *function) const;
  void InitPgStatisticIterator(FunctionBuilder *function) const;
  void InitPgStatisticIndexPR(FunctionBuilder *function) const;
  void AssignColumnStatistics(WorkContext *context, FunctionBuilder *function, size_t column_offset) const;
  void DeclarePgStatisticSlot(FunctionBuilder *function, ast::Identifier slot) const;
  void DeclareAndInitPgStatisticUpdater(FunctionBuilder *function) const;
  void DeclarePgStatisticUpdatePr(FunctionBuilder *function) const;
  void DeleteFromPgStatisticTable(FunctionBuilder *function, ast::Identifier slot) const;
  void InitPgStatisticUpdatePR(FunctionBuilder *function) const;
  void SetPgStatisticTablePr(FunctionBuilder *function) const;
  void InsertIntoPgStatisticTable(FunctionBuilder *function) const;
  void DeleteFromPgStatisticIndex(FunctionBuilder *function, ast::Identifier slot,
                                  catalog::index_oid_t index_oid) const;
  void InsertIntoPgStatisticIndex(FunctionBuilder *function, catalog::index_oid_t index_oid) const;
  void FreePgStatisticUpdater(FunctionBuilder *function) const;
  void FreePgStatisticIterator(FunctionBuilder *function) const;

  void FillIndexPrKey(FunctionBuilder *function, ast::Identifier index_projected_row, bool own) const;
};

}  // namespace noisepage::execution::compiler
