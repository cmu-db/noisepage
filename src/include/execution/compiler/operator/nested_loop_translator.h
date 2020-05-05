#pragma once
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Nested Loop Inner translator.
 * Because a nested loop join can be expressed without any special operation, this translator
 * does not do much. It just binds the two sides of the join together through GetOutput.
 */
class NestedLoopLeftTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  NestedLoopLeftTranslator(const terrier::planner::NestedLoopJoinPlanNode *op, CodeGen *codegen)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::NLJOIN_LEFT), op_(op) {}

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
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }

  // Pass through
  void Consume(FunctionBuilder *builder) override { parent_translator_->Consume(builder); }

  // Pass Through
  ast::Expr *GetOutput(uint32_t attr_idx) override { return child_translator_->GetOutput(attr_idx); }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("This translator does not call DeriveExpr");
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::NestedLoopJoinPlanNode *op_;
};

/**
 * Nested Loop Right Translator.
 * Just like the left translator, this one does not do much. However, it needs to produces an if statement
 * for the join predicate.
 */
class NestedLoopRightTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   * @param left The corresponding left translator
   */
  NestedLoopRightTranslator(const terrier::planner::NestedLoopJoinPlanNode *op, CodeGen *codegen,
                            OperatorTranslator *left)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::NLJOIN_RIGHT),
        op_(op),
        left_(dynamic_cast<NestedLoopLeftTranslator *>(left)) {}

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
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }

  // Generator if statement
  void Consume(FunctionBuilder *builder) override {
    auto join_op = static_cast<const terrier::planner::NestedLoopJoinPlanNode *>(op_);
    if (join_op->GetJoinPredicate() != nullptr) {
      // if (join_predicate) {...}
      auto translator = TranslatorFactory::CreateExpressionTranslator(join_op->GetJoinPredicate().Get(), codegen_);
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
    auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
    return translator->DeriveExpr(this);
  }

  // Pass through to the right child.
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    if (child_idx == 1) {
      return left_->GetOutput(attr_idx);
    }
    return child_translator_->GetOutput(attr_idx);
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::NestedLoopJoinPlanNode *op_;
  NestedLoopLeftTranslator *left_;
};
}  // namespace terrier::execution::compiler
