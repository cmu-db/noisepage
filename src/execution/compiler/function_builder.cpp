#include "execution/compiler/function_builder.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/compiler/codegen.h"

namespace noisepage::execution::compiler {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&params,
                                 ast::Expr *ret_type)
    : codegen_(codegen),
      name_(name),
      params_(std::move(params)),
      ret_type_(ret_type),
      start_(codegen->GetPosition()),
      statements_(codegen->MakeEmptyBlock()),
      is_lambda_(false) {}

FunctionBuilder::FunctionBuilder(CodeGen *codegen, util::RegionVector<ast::FieldDecl *> &&params, ast::Expr *ret_type)
    : codegen_(codegen),
      params_(std::move(params)),
      ret_type_(ret_type),
      start_(codegen->GetPosition()),
      statements_(codegen->MakeEmptyBlock()),
      is_lambda_(true) {}

FunctionBuilder::~FunctionBuilder() { Finish(); }

ast::Expr *FunctionBuilder::GetParameterByPosition(uint32_t param_idx) {
  if (param_idx < params_.size()) {
    return codegen_->MakeExpr(params_[param_idx]->Name());
  }
  return nullptr;
}

void FunctionBuilder::Append(ast::Stmt *stmt) {
  // Append the statement to the block.
  statements_->AppendStatement(stmt);
  // Bump line number.
  codegen_->NewLine();
}

void FunctionBuilder::Append(ast::Expr *expr) { Append(codegen_->GetFactory()->NewExpressionStmt(expr)); }

void FunctionBuilder::Append(ast::VariableDecl *decl) { Append(codegen_->GetFactory()->NewDeclStmt(decl)); }

ast::FunctionDecl *FunctionBuilder::Finish(ast::Expr *ret) {
  if (decl_.fn_decl_ != nullptr) {
    return decl_.fn_decl_;
  }

  NOISEPAGE_ASSERT(ret == nullptr || statements_->IsEmpty() || !statements_->GetLast()->IsReturnStmt(),
                   "Double-return at end of function. You should either call FunctionBuilder::Finish() "
                   "with an explicit return expression, or use the factory to manually append a return "
                   "statement and call FunctionBuilder::Finish() with a null return.");

  // Add the return.
  if (!statements_->IsEmpty() && !statements_->GetLast()->IsReturnStmt()) {
    Append(codegen_->GetFactory()->NewReturnStmt(codegen_->GetPosition(), ret));
  }

  // Finalize everything.
  statements_->SetRightBracePosition(codegen_->GetPosition());

  // Build the function's type.
  auto func_type = codegen_->GetFactory()->NewFunctionType(start_, std::move(params_), ret_type_);

  // Create the declaration.
  auto func_lit = codegen_->GetFactory()->NewFunctionLitExpr(func_type, statements_);
  decl_.fn_decl_ = codegen_->GetFactory()->NewFunctionDecl(start_, name_, func_lit);

  // Done
  return decl_.fn_decl_;
}

noisepage::execution::ast::LambdaExpr *FunctionBuilder::FinishLambda(util::RegionVector<ast::Expr *> &&captures,
                                                                     ast::Expr *ret) {
  NOISEPAGE_ASSERT(is_lambda_, "Asking to finish a lambda function that's not actually a lambda function");
  if (decl_.lambda_expr_ != nullptr) {
    return decl_.lambda_expr_;
  }

  NOISEPAGE_ASSERT(ret == nullptr || statements_->IsEmpty() || !statements_->GetLast()->IsReturnStmt(),
                   "Double-return at end of function. You should either call FunctionBuilder::Finish() "
                   "with an explicit return expression, or use the factory to manually append a return "
                   "statement and call FunctionBuilder::Finish() with a null return.");
  // Add the return.
  if (!statements_->IsEmpty() && !statements_->GetLast()->IsReturnStmt()) {
    Append(codegen_->GetFactory()->NewReturnStmt(codegen_->GetPosition(), ret));
  }
  // Finalize everything.
  statements_->SetRightBracePosition(codegen_->GetPosition());
  // Build the function's type.
  auto func_type = codegen_->GetFactory()->NewFunctionType(start_, std::move(params_), ret_type_);

  // Create the declaration.
  auto func_lit = codegen_->GetFactory()->NewFunctionLitExpr(func_type, statements_);
  decl_.lambda_expr_ = codegen_->GetFactory()->NewLambdaExpr(start_, func_lit, std::move(captures));

  // Done
  return decl_.lambda_expr_;
}

}  // namespace noisepage::execution::compiler
