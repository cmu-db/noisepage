#pragma once

#include <utility>
#include <vector>
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/projection_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Projection Translator
 * The translator only implements GetOutput and GetChildOutput.
 */
class ProjectionTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   * @param pipeline The pipeline this translator is a part of
   */
  ProjectionTranslator(const terrier::planner::ProjectionPlanNode *op, CodeGen *codegen, Pipeline *pipeline)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::PROJECTION, pipeline), op_(op) {}

  // Pass through
  void Produce(FunctionBuilder *builder) override { child_translator_->Produce(builder); }

  // Pass through
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }

  // Pass through
  void Consume(FunctionBuilder *builder) override { parent_translator_->Consume(builder); }

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

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

  ast::Expr *GetOutput(uint32_t attr_idx) override {
    auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
    auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
    return translator->DeriveExpr(this);
  }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return child_translator_->GetOutput(attr_idx);
  }

  // Is always vectorizable.
  bool IsVectorizable() override { return true; }

  // Should not be called here
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override {
    UNREACHABLE("Projection nodes should not use column value expressions");
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::ProjectionPlanNode *op_;
};

}  // namespace terrier::execution::compiler
