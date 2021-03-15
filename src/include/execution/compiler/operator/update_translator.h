#pragma once

#include <vector>

#include "execution/ast/identifier.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::planner {
class UpdatePlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * Update Translator
 */
class UpdateTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given update plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  UpdateTranslator(const planner::UpdatePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Does nothing.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Initialize the storage interface and counters.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Implement update logic where it fills in the update PR obtained from the StorageInterface struct
   * with values from the child and then updates using this the table and all concerned indexes.
   * If this is an indexed update, we do a delete followed by an insert.
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /** Tear down the storage interface. */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /** Record the counters for Lin's models. */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return An expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("Update is serial."); };

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Update is serial.");
  };

 private:
  // Generates the update on the table.
  void GenTableUpdate(FunctionBuilder *builder) const;

  // Declares the storage interface struct used to update.
  void DeclareUpdater(FunctionBuilder *builder) const;

  // Frees the storage interface struct used to update.
  void GenUpdaterFree(FunctionBuilder *builder) const;

  // Sets the columns oids that we are updating on.
  void SetOids(FunctionBuilder *builder) const;

  // Declares the projected row that we will be filling in and updating with.
  void DeclareUpdatePR(FunctionBuilder *builder) const;

  // Gets the projected row from the storage interface that we will be updating with.
  void GetUpdatePR(FunctionBuilder *builder) const;

  // Sets the values in the projected row that we are using to update.
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context) const;

  // Inserts this projected row into the table (used for indexed updates).
  void GenTableInsert(FunctionBuilder *builder) const;

  // Inserts into all indexes.
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

  // Deletes from the table (used for indexed updates).
  void GenTableDelete(FunctionBuilder *builder) const;

  // Deletes from all indexes.
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

  static std::vector<catalog::col_oid_t> CollectOids(const catalog::Schema &schema);

 private:
  // Storage interface for updates.
  StateDescriptor::Entry si_updater_;

  // Projected row that we use to update.
  ast::Identifier update_pr_;

  // Column oids that we are updating on (array).
  ast::Identifier col_oids_;

  // Schema of the table we are updating.
  const catalog::Schema &table_schema_;

  // All the column oids of the table we ae updating.
  std::vector<catalog::col_oid_t> all_oids_;

  // Projection map of the table that we are updating.
  // This maps column oids to offsets in a projected row.
  storage::ProjectionMap table_pm_;

  // The number of updates that are performed.
  StateDescriptor::Entry num_updates_;
};

}  // namespace noisepage::execution::compiler
