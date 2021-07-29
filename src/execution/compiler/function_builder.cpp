#include "execution/compiler/function_builder.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"

namespace noisepage::execution::compiler {

FunctionBuilder::FunctionBuilder(CodeGen *codegen, ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&params,
                                 ast::Expr *return_type)
    : type_{FunctionType::FUNCTION},
      codegen_{codegen},
      name_{name},
      params_{std::move(params)},
      captures_{codegen_->GetAstContext()->GetRegion()},
      return_type_{return_type},
      start_{codegen->GetPosition()},
      statements_{codegen->MakeEmptyBlock()},
      decl_{std::in_place_type<ast::FunctionDecl *>, nullptr} {}

FunctionBuilder::FunctionBuilder(CodeGen *codegen, util::RegionVector<ast::FieldDecl *> &&params,
                                 util::RegionVector<ast::Expr *> &&captures, ast::Expr *return_type)
    : type_{FunctionType::CLOSURE},
      codegen_{codegen},
      params_{std::move(params)},
      captures_{std::move(captures)},
      return_type_{return_type},
      start_{codegen->GetPosition()},
      statements_{codegen->MakeEmptyBlock()},
      decl_{std::in_place_type<ast::LambdaExpr *>, nullptr} {}

FunctionBuilder::~FunctionBuilder() {
  if (type_ == FunctionType::FUNCTION) {
    Finish();
  }
}

ast::Expr *FunctionBuilder::GetParameterByPosition(const std::size_t param_idx) {
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
  NOISEPAGE_ASSERT(type_ == FunctionType::FUNCTION,
                   "Attempt to call FunctionBuilder::Finish on non-function-type builder");
  NOISEPAGE_ASSERT(std::holds_alternative<ast::FunctionDecl *>(decl_), "Broken invariant");
  auto *declaration = std::get<ast::FunctionDecl *>(decl_);
  if (declaration != nullptr) {
    return declaration;
  }

  NOISEPAGE_ASSERT(ret == nullptr || statements_->IsEmpty() || !statements_->GetLast()->IsReturnStmt(),
                   "Double-return at end of function. You should either call FunctionBuilder::Finish() "
                   "with an explicit return expression, or use the factory to manually append a return "
                   "statement and call FunctionBuilder::Finish() with a null return.");

  // Add the return
  if (!statements_->IsEmpty() && !statements_->GetLast()->IsReturnStmt()) {
    Append(codegen_->GetFactory()->NewReturnStmt(codegen_->GetPosition(), ret));
  }

  // Finalize everything
  statements_->SetRightBracePosition(codegen_->GetPosition());

  // Build the function's type
  auto func_type = codegen_->GetFactory()->NewFunctionType(start_, std::move(params_), return_type_);

  // Create the declaration
  auto func_lit = codegen_->GetFactory()->NewFunctionLitExpr(func_type, statements_);
  decl_ = codegen_->GetFactory()->NewFunctionDecl(start_, name_, func_lit);
  return std::get<ast::FunctionDecl *>(decl_);
}

noisepage::execution::ast::LambdaExpr *FunctionBuilder::FinishClosure(ast::Expr *ret) {
  NOISEPAGE_ASSERT(type_ == FunctionType::CLOSURE,
                   "Attempt to call FuncionBuilder::FinishClosure on non-closure-type builder");
  NOISEPAGE_ASSERT(std::holds_alternative<ast::LambdaExpr *>(decl_), "Broken invariant");
  auto *declaration = std::get<ast::LambdaExpr *>(decl_);
  if (declaration != nullptr) {
    return declaration;
  }

  NOISEPAGE_ASSERT(ret == nullptr || statements_->IsEmpty() || !statements_->GetLast()->IsReturnStmt(),
                   "Double-return at end of function. You should either call FunctionBuilder::FinishClosure() "
                   "with an explicit return expression, or use the factory to manually append a return "
                   "statement and call FunctionBuilder::FinishClosure() with a null return.");
  // Add the return
  if (!statements_->IsEmpty() && !statements_->GetLast()->IsReturnStmt()) {
    Append(codegen_->GetFactory()->NewReturnStmt(codegen_->GetPosition(), ret));
  }
  // Finalize everything
  statements_->SetRightBracePosition(codegen_->GetPosition());
  // Build the function's type
  auto func_type = codegen_->GetFactory()->NewFunctionType(start_, std::move(params_), return_type_);

  // Create the declaration
  auto func_lit = codegen_->GetFactory()->NewFunctionLitExpr(func_type, statements_);
  decl_ = codegen_->GetFactory()->NewLambdaExpr(start_, func_lit, std::move(captures_));
  return std::get<ast::LambdaExpr *>(decl_);
}

}  // namespace noisepage::execution::compiler
