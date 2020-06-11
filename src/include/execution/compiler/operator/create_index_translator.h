#pragma once
#include <vector>
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/create_index_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Index Insert Translator
 */
class CreateIndexTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen);

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

  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Inserts don't output anything"); };
  const planner::AbstractPlanNode *Op() override { return op_; }
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the index_inserter
  void DeclareIndexInserter(FunctionBuilder *builder);
  void GenIndexInserterFree(FunctionBuilder *builder);
  // Insert into table.
  void GenCreateIndex(FunctionBuilder *builder);


 private:
  const planner::CreateIndexPlanNode *op_;
  ast::Identifier index_inserter_;
  ast::Identifier col_oids_;

};

}  // namespace terrier::execution::compiler
