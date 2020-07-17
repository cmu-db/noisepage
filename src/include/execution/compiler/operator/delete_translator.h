#pragma once

#include <vector>

#include "catalog/schema.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/delete_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Delete Translator
 */
class DeleteTranslator : public OperatorTranslator {
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
   * Does nothing
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Does nothing
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Implement deletion logic where it fills in the delete PR obtained from the StorageInterface struct
   * with values from the child and then deletes using this from the table and all concerned indexes
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Unreachable
   * @param col_oid column oid to return a value for
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Delete doesn't provide values"); }

 private:

  // Declare the deleter storageinterface
  void DeclareDeleter(FunctionBuilder *builder) const;

  // free the delete storageinterface
  void GenDeleterFree(FunctionBuilder *builder) const;

  // sets the oids we are inserting on using schema from the delete plan node
  void SetOids(FunctionBuilder *builder) const;

  // generates code to delete from the table
  void GenTableDelete(FunctionBuilder *builder) const;

  // generates code to delete from the indexes
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

 private:
  // deleter storageinterface struct
  ast::Identifier deleter_;

  // column oid's of the table we are deleting from
  ast::Identifier col_oids_;
};

}  // namespace terrier::execution::compiler
