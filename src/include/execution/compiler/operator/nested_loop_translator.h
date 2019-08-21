#pragma once
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace terrier::execution::compiler {

class NestedLoopRightTranslator;

/**
 * Nested Loop Inner translator.
 * Because a nested loop join can be expressed without any special operation, this translator
 * does not do much. It just binds the two sides of the join together through GetOutput.
 */
class NestedLoopLeftTransaltor : public OperatorTranslator {
 public:
  NestedLoopLeftTransaltor(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen)
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

  // Pass through
  void Produce(FunctionBuilder *builder) override { child_translator_->Produce(builder); }

  // Pass through
  void Consume(FunctionBuilder *builder) override { parent_translator_->Consume(builder); }

  // Pass Through
  ast::Expr *GetOutput(uint32_t attr_idx) override { return child_translator_->GetOutput(attr_idx); }

  // TODO(Amadou): Confirm that this is never call, since DeriveExpr is not called here.
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
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
  NestedLoopRightTransaltor(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen, OperatorTranslator *left)
      : OperatorTranslator(op, codegen), left_(dynamic_cast<NestedLoopLeftTransaltor *>(left)) {}

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

  // Pass through
  void Produce(FunctionBuilder *builder) override { child_translator_->Produce(builder); }

  // Generator if statement
  void Consume(FunctionBuilder *builder) override {
    auto join_op = static_cast<const terrier::planner::NestedLoopJoinPlanNode *>(op_);
    if (join_op->GetJoinPredicate() != nullptr) {
      // if (join_predicate) {...}
      auto translator = TranslatorFactory::CreateExpressionTranslator(join_op->GetJoinPredicate().get(), codegen_);
      ast::Expr *predicate = translator->DeriveExpr(this);
      builder->StartIfStmt(predicate);
      // Let parent consume
      parent_translator_->Consume(builder);
      // Close if statement
      builder->FinishBlockStmt();
    } else {
      // Directly let parent consume
      parent_translator_->Consume(builder);
    }
  }

  // Return the output at the given index
  ast::Expr *GetOutput(uint32_t attr_idx) override {
    auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
    auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
    return translator->DeriveExpr(this);
  }

  // Pass through to the right child.
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    if (child_idx == 1) {
      return left_->GetOutput(attr_idx);
    }
    return child_translator_->GetOutput(attr_idx);
  }

 private:
  NestedLoopLeftTransaltor *left_;
};
}  // namespace terrier::execution::compiler