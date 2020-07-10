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
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  DeleteTranslator(const planner::DeletePlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

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
  //   ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

  /**
   * @return an expression representing the value of the column with the given OID.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override { UNREACHABLE("Delete doesn't provide values"); }

 private:
  void DeclareDeleter(FunctionBuilder *builder) const;
  void GenDeleterFree(FunctionBuilder *builder) const;
  void SetOids(FunctionBuilder *builder) const;
  void GenTableDelete(FunctionBuilder *builder) const;
  void GenIndexDelete(FunctionBuilder *builder, WorkContext *context, const catalog::index_oid_t &index_oid) const;

  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  ast::Identifier deleter_;
  ast::Identifier col_oids_;
  std::vector<catalog::col_oid_t> oids_;
};

}  // namespace terrier::execution::compiler
