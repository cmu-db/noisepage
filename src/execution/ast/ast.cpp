#include "execution/ast/ast.h"

#include "execution/ast/type.h"

namespace tpl::ast {

// ---------------------------------------------------------
// Function Declaration
// ---------------------------------------------------------

FunctionDecl::FunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *func)
    : Decl(Kind::FunctionDecl, pos, name, func->type_repr()), func_(func) {}

// ---------------------------------------------------------
// Structure Declaration
// ---------------------------------------------------------

StructDecl::StructDecl(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr)
    : Decl(Kind::StructDecl, pos, name, type_repr) {}

// ---------------------------------------------------------
// Expression Statement
// ---------------------------------------------------------

ExpressionStmt::ExpressionStmt(Expr *expr) : Stmt(Kind::ExpressionStmt, expr->position()), expr_(expr) {}

// ---------------------------------------------------------
// Function Literal Expressions
// ---------------------------------------------------------

FunctionLitExpr::FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body)
    : Expr(Kind::FunctionLitExpr, type_repr->position()), type_repr_(type_repr), body_(body) {}

// ---------------------------------------------------------
// Call Expression
// ---------------------------------------------------------

Identifier CallExpr::GetFuncName() const { return func_->As<IdentifierExpr>()->name(); }

// ---------------------------------------------------------
// Index Expressions
// ---------------------------------------------------------

bool IndexExpr::IsArrayAccess() const {
  TPL_ASSERT(object() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(object() != nullptr, "Cannot determine object type before type checking!");
  return object()->type()->IsArrayType();
}

bool IndexExpr::IsMapAccess() const {
  TPL_ASSERT(object() != nullptr, "Object cannot be NULL");
  TPL_ASSERT(object() != nullptr, "Cannot determine object type before type checking!");
  return object()->type()->IsMapType();
}

// ---------------------------------------------------------
// Member expression
// ---------------------------------------------------------

bool MemberExpr::IsSugaredArrow() const {
  TPL_ASSERT(object()->type() != nullptr, "Cannot determine sugared-arrow before type checking!");
  return object()->type()->IsPointerType();
}

// ---------------------------------------------------------
// Statement
// ---------------------------------------------------------

bool Stmt::IsTerminating(Stmt *stmt) {
  switch (stmt->kind()) {
    case AstNode::Kind::BlockStmt: {
      return IsTerminating(stmt->As<BlockStmt>()->statements().back());
    }
    case AstNode::Kind::IfStmt: {
      auto *if_stmt = stmt->As<IfStmt>();
      return (if_stmt->HasElseStmt() && (IsTerminating(if_stmt->then_stmt()) && IsTerminating(if_stmt->else_stmt())));
    }
    case AstNode::Kind::ReturnStmt: {
      return true;
    }
    default: { return false; }
  }
}

}  // namespace tpl::ast
