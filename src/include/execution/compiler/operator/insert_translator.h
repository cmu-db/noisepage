#pragma once
#include <vector>
#include <catalog/schema.h>
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::planner {
class InsertPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::compiler {

/**
 * Insert Translator
 */
class InsertTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

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

  /**
   * Tear-down the inserter if required.
   */
//  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override;

//  void Abort(FunctionBuilder *builder) const override;

//  /**
//   * @return The pipeline work function parameters. Just the *TVI.
//   */
//  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;
//
//  /**
//   * Launch a parallel table scan.
//   * @param work_func The worker function that'll be called during the parallel scan.
//   */
//  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override;

 private:
  // Declare the inserter
  void DeclareInserter(FunctionBuilder *builder) const;
  void GenInserterFree(FunctionBuilder *builder) const;
  // Set the oids variable
  void SetOids(FunctionBuilder *builder) const;
  // Declare the insert PR
  void DeclareInsertPR(FunctionBuilder *builder) const;
  // Get the pr to insert
  void GetInsertPR(FunctionBuilder *builder) const;
  // Fill the insert PR from the child's output
  void FillPRFromChild(FunctionBuilder *builder) const;
  // Set the table PR from raw values
  void GenSetTablePR(FunctionBuilder *builder, WorkContext *context, uint32_t idx) const;
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder) const;
  // Insert into index.
  void GenIndexInsert(WorkContext *context, FunctionBuilder *builder, const catalog::index_oid_t &index_oid) const;
  // Get all columns oids.
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
//  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
