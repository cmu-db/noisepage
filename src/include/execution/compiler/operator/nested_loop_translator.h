#pragma once
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/function_builder.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace tpl::compiler {

class NestedLoopRightTranslator;

/**
 * Nested Loop Left translator.
 * Because a nested loop join can be expressed without any special operation, this translator
 * does not produce anything. It just binds the two sides of the join together through GetOutput.
 */
class NestedLoopLeftTransaltor : public OperatorTranslator {
 public:
  NestedLoopLeftTransaltor(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen)
  : OperatorTranslator(op, codegen) {}

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

  // Does nothing
  void Produce(FunctionBuilder * builder) override {}

  // Pass Through
  ast::Expr * GetOutput(uint32_t attr_idx) override {
    return prev_translator_->GetOutput(attr_idx);
  }

  // TODO(Amadou): Confirm that this is never call, since DeriveExpr is not called here.
  ast::Expr * GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return nullptr;
  }
};

/**
 * Nested Loop Right Translator.
 * Just like the left translator, this one does not do much. However, it needs to produces an if statement
 * for the join predicate.
 */
class NestedLoopRightTransaltor : public OperatorTranslator {
 public:
  NestedLoopRightTransaltor(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen, OperatorTranslator * left)
      : OperatorTranslator(op, codegen)
      , left_(dynamic_cast<NestedLoopLeftTransaltor *>(left)){}

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

  // Generate an if statement for the scan predicate
  void Produce(FunctionBuilder * builder) override {
    // if (join_predicate) {...}
    auto join_op = dynamic_cast<const terrier::planner::NestedLoopJoinPlanNode*>(op_);
    ExpressionTranslator * translator = TranslatorFactory::CreateExpressionTranslator(join_op->GetJoinPredicate().get(), codegen_);
    ast::Expr * predicate = translator->DeriveExpr(this);
    builder->StartIfStmt(predicate);
  }

  // Return the output at the given index
  ast::Expr* GetOutput(uint32_t attr_idx) override {
    auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
    ExpressionTranslator * translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
    return translator->DeriveExpr(this);
  }

  // Pass through to the right child.
  ast::Expr* GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    if (child_idx == 0) {
      return left_->GetOutput(attr_idx);
    }
    return prev_translator_->GetOutput(attr_idx);
  }

 private:
  NestedLoopLeftTransaltor * left_;
};
}