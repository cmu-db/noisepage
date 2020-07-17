#pragma once

#include <catalog/schema.h>

#include <vector>

#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Update Translator
 */
class UpdateTranslator : public OperatorTranslator {
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
   * Implement update logic where it fills in the update PR obtained from the StorageInterface struct
   * with values from the child and then updates using this the table and all concerned indexes
   * If this is an indexed update, we first do a delete then insert
   * @param context The context of the work.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return The value (vector) of the attribute at the given index (@em attr_idx) produced by the
   *         child at the given index (@em child_idx).
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:
  // Generates the update on the table.
  void GenTableUpdate(FunctionBuilder *builder) const;

  // declares the storageinterface struct used to update
  void DeclareUpdater(FunctionBuilder *builder) const;

  // frees the storageinterface struct used to update
  void GenUpdaterFree(FunctionBuilder *builder) const;

  // sets the columns oids that we are updating on
  void SetOids(FunctionBuilder *builder) const;

  // declares the projected row that we will be filling in and updating with
  void DeclareUpdatePR(FunctionBuilder *builder) const;

  // gets the projected row from the storageinterface that we will be updating with
  void GetUpdatePR(FunctionBuilder *builder) const;

  // sets the values in the projected row we are using to update
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context) const;

  // inserts this projected row into the table (used for indexed updates)
  void GenTableInsert(FunctionBuilder *builder) const;

  // inserts into all indexes
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

  // deletes from the table (used for indexed updates)
  void GenTableDelete(FunctionBuilder *builder) const;

  // deletes from all indexes
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

  static std::vector<catalog::col_oid_t> CollectOids(const planner::UpdatePlanNode &node) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &clause : node.GetSetClauses()) {
      oids.emplace_back(clause.first);
    }
    return oids;
  }

 private:
  // storageinterface struct that we are updating with
  ast::Identifier updater_;

  // projected row that we use to update
  ast::Identifier update_pr_;

  // column oids that we are updating on (array)
  ast::Identifier col_oids_;

  // schema of the table we are updating
  const catalog::Schema &table_schema_;

  // all the column oids of the table we ae updating
  std::vector<catalog::col_oid_t> all_oids_;

  // projection map of the table we are updating mapping, coloid to offsets in a
  // projected row
  storage::ProjectionMap table_pm_;
};

}  // namespace terrier::execution::compiler
