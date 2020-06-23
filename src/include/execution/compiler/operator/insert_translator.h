//#pragma once
//#include <vector>
//#include "execution/compiler/expression/pr_filler.h"
//#include "execution/compiler/operator/operator_translator.h"
//#include "planner/plannodes/insert_plan_node.h"
//
//namespace terrier::execution::compiler {
//
///**
// * Insert Translator
// */
//class InsertTranslator : public OperatorTranslator {
// public:
//  /**
//   * Constructor
//   * @param op The plan node
//   * @param codegen The code generator
//   */
//  InsertTranslator(const planner::InsertPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);
//
//  /**
//   * If the scan has a predicate, this function will define all clause functions.
//   * @param decls The top-level declarations.
//   */
//  void DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) override {}
//
//  /**
//   * Initialize the FilterManager if required.
//   */
//  void InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const override {}
//
//  /**
//   * Generate the scan.
//   * @param context The context of the work.
//   */
//  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;
//
//  /**
//   * Tear-down the FilterManager if required.
//   */
//  void TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *func) const override;
//
////  /**
////   * @return The pipeline work function parameters. Just the *TVI.
////   */
////  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override;
////
////  /**
////   * Launch a parallel table scan.
////   * @param work_func The worker function that'll be called during the parallel scan.
////   */
////  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) const override;
//
//  /**
//   * @return The value (or value vector) of the column with the provided column OID in the table
//   *         this sequential scan is operating over.
//   */
//  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;
//
// private:
//  // Declare the inserter
//  void DeclareInserter(FunctionBuilder *builder) const;
//  void GenInserterFree(FunctionBuilder *builder);
//  // Set the oids variable
//  void SetOids(FunctionBuilder *builder);
//  // Declare the insert PR
//  void DeclareInsertPR(FunctionBuilder *builder) const;
//  // Get the pr to insert
//  void GetInsertPR(FunctionBuilder *builder);
//  // Fill the insert PR from the child's output
//  void FillPRFromChild(FunctionBuilder *builder);
//  // Set the table PR from raw values
//  void GenSetTablePR(FunctionBuilder *builder, uint32_t idx);
//  // Insert into table.
//  void GenTableInsert(FunctionBuilder *builder);
//  // Insert into index.
//  void GenIndexInsert(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);
//  // Get all columns oids.
//  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
//    std::vector<catalog::col_oid_t> oids;
//    for (const auto &col : table_schema_.GetColumns()) {
//      oids.emplace_back(col.Oid());
//    }
//    return oids;
//  }
//
// private:
//  const planner::InsertPlanNode *op_;
//  ast::Identifier inserter_;
//  ast::Identifier insert_pr_;
//  ast::Identifier col_oids_;
//
//  const catalog::Schema &table_schema_;
//  std::vector<catalog::col_oid_t> all_oids_;
//  storage::ProjectionMap table_pm_;
//  PRFiller pr_filler_;
//};
//
//}  // namespace terrier::execution::compiler
