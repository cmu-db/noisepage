#include <memory>

#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

void Sema::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  ast::Type *src_type = Resolve(node->source());
  ast::Type *dest_type = Resolve(node->destination());

  if (src_type == nullptr || dest_type == nullptr) {
    return;
  }

  // Check assignment
  ast::Expr *source = node->source();
  if (!CheckAssignmentConstraints(dest_type, &source)) {
    error_reporter_->Report(node->position(), ErrorMessages::kInvalidAssignment, src_type, dest_type);
    return;
  }

  // Assignment looks good, but the source may have been casted
  if (source != node->source()) {
    node->set_source(source);
  }
}

void Sema::VisitBlockStmt(ast::BlockStmt *node) {
  SemaScope block_scope(this, Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void Sema::VisitFile(ast::File *node) {
  SemaScope file_scope(this, Scope::Kind::File);

  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
}

void Sema::VisitForStmt(ast::ForStmt *node) {
  // Create a new scope for variables introduced in initialization block
  SemaScope for_scope(this, Scope::Kind::Loop);

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  if (node->condition() != nullptr) {
    ast::Type *cond_type = Resolve(node->condition());
    // If unable to resolve condition type, there was some error
    if (cond_type == nullptr) {
      return;
    }
    // If the resolved type isn't a boolean, it's an error
    if (!cond_type->IsBoolType()) {
      error_reporter_->Report(node->condition()->position(), ErrorMessages::kNonBoolForCondition);
    }
  }

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  // The body
  Visit(node->body());
}

void Sema::VisitForInStmt(ast::ForInStmt *node) { TPL_ASSERT(false, "Not supported"); }

void Sema::VisitExpressionStmt(ast::ExpressionStmt *node) { Visit(node->expression()); }

void Sema::VisitIfStmt(ast::IfStmt *node) {
  if (ast::Type *cond_type = Resolve(node->condition()); cond_type == nullptr) {
    // Error
    return;
  }

  // If the result type of the evaluated condition is a SQL boolean value, we
  // implicitly cast it to a native boolean value before we feed it into the
  // if-condition.

  if (node->condition()->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    // A primitive boolean
    auto *bool_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);

    // Perform implicit cast from SQL boolean to primitive boolean
    ast::Expr *cond = node->condition();
    cond =
        context()->node_factory()->NewImplicitCastExpr(cond->position(), ast::CastKind::SqlBoolToBool, bool_type, cond);
    cond->set_type(bool_type);
    node->set_condition(cond);
  }

  // If the conditional isn't an explicit boolean type, error
  if (!node->condition()->type()->IsBoolType()) {
    error_reporter_->Report(node->condition()->position(), ErrorMessages::kNonBoolIfCondition);
  }

  Visit(node->then_stmt());

  if (node->else_stmt() != nullptr) {
    Visit(node->else_stmt());
  }
}

void Sema::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->declaration()); }

void Sema::VisitReturnStmt(ast::ReturnStmt *node) {
  if (current_function() == nullptr) {
    error_reporter_->Report(node->position(), ErrorMessages::kReturnOutsideFunction);
    return;
  }

  // If there's an expression with the return clause, resolve it now. We'll
  // check later if we need it.

  ast::Type *return_type = nullptr;
  if (node->ret() != nullptr) {
    return_type = Resolve(node->ret());
  }

  // If the function has a nil-type, we just need to make sure this return
  // statement doesn't have an attached expression. If it does, that's an error

  auto *func_type = current_function()->type()->As<ast::FunctionType>();

  if (func_type->return_type()->IsNilType()) {
    if (return_type != nullptr) {
      error_reporter_->Report(node->position(), ErrorMessages::kMismatchedReturnType, return_type,
                              func_type->return_type());
    }
    return;
  }

  // The function has a non-nil return type. So, we need to make sure the
  // resolved type of the expression in this return is compatible with the
  // return type of the function.

  if (return_type == nullptr) {
    error_reporter_->Report(node->position(), ErrorMessages::kMissingReturn);
    return;
  }

  ast::Expr *ret = node->ret();
  if (!CheckAssignmentConstraints(func_type->return_type(), &ret)) {
    error_reporter_->Report(node->position(), ErrorMessages::kMismatchedReturnType, return_type,
                            func_type->return_type());
    return;
  }

  // Cast if necessary
  if (ret != node->ret()) {
    node->set_return(ret);
  }
}

}  // namespace terrier::execution::sema
