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
 * Note that child_translator_ may be nullptr in certain cases, e.g., "SELECT 1".
 */
class ProjectionTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  ProjectionTranslator(const terrier::planner::ProjectionPlanNode *op, CodeGen *codegen)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::PROJECTION), op_(op) {}

  // Pass through
  void Produce(FunctionBuilder *builder) override {
    if (LIKELY(nullptr != child_translator_)) {
      child_translator_->Produce(builder);
    } else {
      // For queries like "SELECT 1", the parent translator will consume the ProjectionTranslator's output directly.
      parent_translator_->Consume(builder);
    }
  }

  // Pass through
  void Abort(FunctionBuilder *builder) override {
    if (LIKELY(nullptr != child_translator_)) {
      child_translator_->Abort(builder);
    }
  }

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
