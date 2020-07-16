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
   * Constructor
   * @param plan The plan node
   * @param compilation_context The compilation context
   * @param pipeline The pipeline
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
   * @param function The pipeline generating function.
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
  // Update on table.
  void GenTableUpdate(FunctionBuilder *builder) const;

  void DeclareUpdater(FunctionBuilder *builder) const;
  void GenUpdaterFree(FunctionBuilder *builder) const;
  void SetOids(FunctionBuilder *builder) const;
  void DeclareUpdatePR(FunctionBuilder *builder) const;
  void GetUpdatePR(FunctionBuilder *builder) const;
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context) const;
  void GenTableInsert(FunctionBuilder *builder) const;
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;
  void GenTableDelete(FunctionBuilder *builder) const;
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

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
};

}  // namespace terrier::execution::compiler
