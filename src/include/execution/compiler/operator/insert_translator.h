#pragma once

#include <vector>

#include "catalog/schema.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::planner {
class InsertPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

/**
 * InsertTranslator
 */
class InsertTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param plan The plan node
   * @param compilation_context The compilation context
   * @param pipeline The pipeline
   */
  InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * If the scan has a predicate, this function will define all clause functions.
   * @param decls The top-level declarations.
   */
  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}

  /**
   * Initialize the FilterManager if required.
   * @param pipeline The current pipeline.
   * @param function The pipeline generating function.
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
  void DeclareInserter(FunctionBuilder *builder) const;
  void GenInserterFree(FunctionBuilder *builder) const;
  void SetOids(FunctionBuilder *builder) const;
  void DeclareInsertPR(FunctionBuilder *builder) const;
  void GetInsertPR(FunctionBuilder *builder) const;
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const;
  void GenTableInsert(FunctionBuilder *builder) const;
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;

  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  ast::Identifier inserter_;
  ast::Identifier insert_pr_;
  ast::Identifier col_oids_;

  const catalog::Schema &table_schema_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
};

}  // namespace terrier::execution::compiler
