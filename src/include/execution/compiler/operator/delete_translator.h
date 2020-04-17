#pragma once

#include <vector>
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
   * @param pipeline The pipeline this translator is a part of
   */
  DeleteTranslator(const terrier::planner::DeletePlanNode *op, CodeGen *codegen, Pipeline *pipeline);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override{};

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override{};

  // Produce and consume logic
  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  /**
   * @return The pipeline work function parameters
   */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() override { UNREACHABLE("Not implemented yet"); }

  /**
   * @param function The caller function
   * @param work_func The worker function that'll be called
   */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func) override {
    UNREACHABLE("LaunchWork for parallel execution is not implemented yet");
  }

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Deletes don't output anything"); };

  const planner::AbstractPlanNode *Op() override { return op_; }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the deleter
  void DeclareDeleter(FunctionBuilder *builder);
  // Free the deleter
  void GenDeleterFree(FunctionBuilder *builder);
  // Set the oids variable
  void SetOids(FunctionBuilder *builder);
  // Delete from table.
  void GenTableDelete(FunctionBuilder *builder);
  // Delete from index.
  void GenIndexDelete(FunctionBuilder *builder, const catalog::index_oid_t &index_oid);
  // Get all columns oids.
  static std::vector<catalog::col_oid_t> AllColOids(const catalog::Schema &table_schema_) {
    std::vector<catalog::col_oid_t> oids;
    for (const auto &col : table_schema_.GetColumns()) {
      oids.emplace_back(col.Oid());
    }
    return oids;
  }

 private:
  const planner::DeletePlanNode *op_;
  ast::Identifier deleter_;
  ast::Identifier col_oids_;

  // TODO(Amadou): If tpl supports null arrays, leave this empty. Otherwise, put a dummy value of 1 inside.
  std::vector<catalog::col_oid_t> oids_;
};

}  // namespace terrier::execution::compiler
