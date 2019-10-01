#include <memory>

#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

void Sema::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  ast::Type *src_type = Resolve(node->Source());
  ast::Type *dest_type = Resolve(node->Destination());

  if (src_type == nullptr || dest_type == nullptr) {
    return;
  }

  // Check assignment
  ast::Expr *source = node->Source();
  if (!CheckAssignmentConstraints(dest_type, &source)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kInvalidAssignment, src_type, dest_type);
    return;
  }

  // Assignment looks good, but the source may have been casted
  if (source != node->Source()) {
    node->SetSource(source);
  }
}

void Sema::VisitBlockStmt(ast::BlockStmt *node) {
  SemaScope block_scope(this, Scope::Kind::Block);

  for (auto *stmt : node->Statements()) {
    Visit(stmt);
  }
}

void Sema::VisitFile(ast::File *node) {
  SemaScope file_scope(this, Scope::Kind::File);

  for (auto *decl : node->Declarations()) {
    Visit(decl);
  }
}

void Sema::VisitForStmt(ast::ForStmt *node) {
  // Create a new scope for variables introduced in initialization block
  SemaScope for_scope(this, Scope::Kind::Loop);

  if (node->Init() != nullptr) {
    Visit(node->Init());
  }

  if (node->Condition() != nullptr) {
    ast::Type *cond_type = Resolve(node->Condition());
    // If unable to resolve condition type, there was some error
    if (cond_type == nullptr) {
      return;
    }
    // If the resolved type isn't a boolean, it's an error
    if (!cond_type->IsBoolType()) {
      error_reporter_->Report(node->Condition()->Position(), ErrorMessages::kNonBoolForCondition);
    }
  }

  if (node->Next() != nullptr) {
    Visit(node->Next());
  }

  // The body
  Visit(node->Body());
}

void Sema::VisitForInStmt(ast::ForInStmt *node) { TERRIER_ASSERT(false, "Not supported"); }

void Sema::VisitExpressionStmt(ast::ExpressionStmt *node) { Visit(node->Expression()); }

void Sema::VisitIfStmt(ast::IfStmt *node) {
  if (ast::Type *cond_type = Resolve(node->Condition()); cond_type == nullptr) {
    // Error
    return;
  }

  // If the result type of the evaluated condition is a SQL boolean value, we
  // implicitly cast it to a native boolean value before we feed it into the
  // if-condition.

  if (node->Condition()->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    // A primitive boolean
    auto *bool_type = ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool);

    // Perform implicit cast from SQL boolean to primitive boolean
    ast::Expr *cond = node->Condition();
    cond = GetContext()->NodeFactory()->NewImplicitCastExpr(cond->Position(), ast::CastKind::SqlBoolToBool, bool_type,
                                                            cond);
    cond->SetType(bool_type);
    node->SetCondition(cond);
  }

  // If the conditional isn't an explicit boolean type, error
  if (!node->Condition()->GetType()->IsBoolType()) {
    error_reporter_->Report(node->Condition()->Position(), ErrorMessages::kNonBoolIfCondition);
  }

  Visit(node->ThenStmt());

  if (node->ElseStmt() != nullptr) {
    Visit(node->ElseStmt());
  }
}

void Sema::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->Declaration()); }

void Sema::VisitReturnStmt(ast::ReturnStmt *node) {
  if (CurrentFunction() == nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kReturnOutsideFunction);
    return;
  }

  // If there's an expression with the return clause, resolve it now. We'll
  // check later if we need it.

  ast::Type *return_type = nullptr;
  if (node->Ret() != nullptr) {
    return_type = Resolve(node->Ret());
  }

  // If the function has a nil-type, we just need to make sure this return
  // statement doesn't have an attached expression. If it does, that's an error

  auto *func_type = CurrentFunction()->GetType()->As<ast::FunctionType>();

  if (func_type->ReturnType()->IsNilType()) {
    if (return_type != nullptr) {
      error_reporter_->Report(node->Position(), ErrorMessages::kMismatchedReturnType, return_type,
                              func_type->ReturnType());
    }
    return;
  }

  // The function has a non-nil return type. So, we need to make sure the
  // resolved type of the expression in this return is compatible with the
  // return type of the function.

  if (return_type == nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kMissingReturn);
    return;
  }

  ast::Expr *ret = node->Ret();
  if (!CheckAssignmentConstraints(func_type->ReturnType(), &ret)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kMismatchedReturnType, return_type,
                            func_type->ReturnType());
    return;
  }

  // Cast if necessary
  if (ret != node->Ret()) {
    node->SetReturn(ret);
  }
}

}  // namespace terrier::execution::sema
