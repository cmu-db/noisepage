#pragma once

#include <vector>

#include "execution/ast/identifier.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"

namespace noisepage::catalog {
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::planner {
class DeletePlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * Delete Translator
 */
class DeleteTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a new translator for the given delete plan. The compilation occurs within the
   * provided compilation context and the operator is participating in the provided pipeline.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  DeleteTranslator(const planner::DeletePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Does nothing.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /** Initialize the storage interface and counters. */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Implement deletion logic where it fills in the delete PR obtained from the StorageInterface struct
   * with values from the child and then deletes using this from the table and all concerned indexes.
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /** Tear down the storage interface. */
  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /** Record the counters. */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Unreachable.
   * @param col_oid Column oid to return a value for.
   * @return An expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Delete doesn't provide values"); }

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("Delete is serial."); };

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Delete is serial.");
  };

 private:
  // Declare the deleter storage interface.
  void DeclareDeleter(FunctionBuilder *builder) const;

  // Free the delete storage interface.
  void GenDeleterFree(FunctionBuilder *builder) const;

  // Sets the oids that we are inserting, using the schema from the delete plan node.
  void SetOids(FunctionBuilder *builder) const;

  // Generates code to delete from the table.
  void GenTableDelete(FunctionBuilder *builder) const;

  // Generates code to delete from the indexes.
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

 private:
  // Storage interface for deletes.
  StateDescriptor::Entry si_deleter_;

  // Column oids of the table we are deleting from.
  ast::Identifier col_oids_;

  // The number of deletes that are performed.
  StateDescriptor::Entry num_deletes_;
};

}  // namespace noisepage::execution::compiler
