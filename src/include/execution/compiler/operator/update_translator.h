#pragma once

#include <catalog/schema.h>

#include <vector>

#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Update Translator
 */
class UpdateTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  UpdateTranslator(const planner::UpdatePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Initialize the FilterManager if required.
   */
  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}

  /**
   * Generate the scan.
   * @param context The context of the work.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * @return the child's output at the given index
   */
  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:
  // Declare the updater
  void DeclareUpdater(FunctionBuilder *builder) const;
  // Free the updater
  void GenUpdaterFree(FunctionBuilder *builder) const;
  // Set the oids variable
  void SetOids(FunctionBuilder *builder) const;
  // Declare the update PR
  void DeclareUpdatePR(FunctionBuilder *builder) const;
  // Get the pr to update
  void GetUpdatePR(FunctionBuilder *builder) const;
  // Fill the update PR from the child's output
  void FillPRFromChild(WorkContext *context, FunctionBuilder *builder) const;
  // Update on table.
  void GenTableUpdate(FunctionBuilder *builder) const;
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder) const;
  // Insert into index.
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;
  // Delete from table.
  void GenTableDelete(FunctionBuilder *builder) const;
  // Delete from index.
  void GenIndexDelete(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

  // Get all columns oids.
  static std::vector<catalog::col_oid_t> CollectOids(const planner::UpdatePlanNode &node) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &clause : node.GetSetClauses()) {
      oids.emplace_back(clause.first);
    }
    return oids;
  }

 private:
  ast::Identifier updater_;
  ast::Identifier update_pr_;
  ast::Identifier col_oids_;

  const catalog::Schema &table_schema_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
  //  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
