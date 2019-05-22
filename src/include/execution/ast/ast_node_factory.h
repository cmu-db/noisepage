#pragma once

#include <utility>

#include "execution/ast/ast.h"
#include "execution/util/region.h"

namespace tpl::ast {

/**
 * A factory for AST nodes. This factory uses a region allocator to quickly
 * allocate AST nodes during parsing. The assumption here is that the nodes are
 * only required during parsing and are thrown away after code generation, hence
 * require quick deallocation as well, thus the use of a region.
 */
class AstNodeFactory {
 public:
  explicit AstNodeFactory(util::Region *region) : region_(region) {}

  DISALLOW_COPY_AND_MOVE(AstNodeFactory);

  File *NewFile(const SourcePosition &pos, util::RegionVector<Decl *> &&declarations) {
    return new (region_) File(pos, std::move(declarations));
  }

  FunctionDecl *NewFunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *fun) {
    return new (region_) FunctionDecl(pos, name, fun);
  }

  StructDecl *NewStructDecl(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr) {
    return new (region_) StructDecl(pos, name, type_repr);
  }

  VariableDecl *NewVariableDecl(const SourcePosition &pos, Identifier name, Expr *type_repr, Expr *init) {
    return new (region_) VariableDecl(pos, name, type_repr, init);
  }

  BlockStmt *NewBlockStmt(const SourcePosition &start_pos, const SourcePosition &end_pos,
                          util::RegionVector<Stmt *> &&statements) {
    return new (region_) BlockStmt(start_pos, end_pos, std::move(statements));
  }

  DeclStmt *NewDeclStmt(Decl *decl) { return new (region_) DeclStmt(decl); }

  AssignmentStmt *NewAssignmentStmt(const SourcePosition &pos, Expr *dest, Expr *src) {
    return new (region_) AssignmentStmt(pos, dest, src);
  }

  ExpressionStmt *NewExpressionStmt(Expr *expression) { return new (region_) ExpressionStmt(expression); }

  ForStmt *NewForStmt(const SourcePosition &pos, Stmt *init, Expr *cond, Stmt *next, BlockStmt *body) {
    return new (region_) ForStmt(pos, init, cond, next, body);
  }

  ForInStmt *NewForInStmt(const SourcePosition &pos, Expr *target, Expr *iter, Attributes *attributes,
                          BlockStmt *body) {
    return new (region_) ForInStmt(pos, target, iter, attributes, body);
  }

  IfStmt *NewIfStmt(const SourcePosition &pos, Expr *cond, BlockStmt *then_stmt, Stmt *else_stmt) {
    return new (region_) IfStmt(pos, cond, then_stmt, else_stmt);
  }

  ReturnStmt *NewReturnStmt(const SourcePosition &pos, Expr *ret) { return new (region_) ReturnStmt(pos, ret); }

  BadExpr *NewBadExpr(const SourcePosition &pos) { return new (region_) BadExpr(pos); }

  BinaryOpExpr *NewBinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right) {
    return new (region_) BinaryOpExpr(pos, op, left, right);
  }

  ComparisonOpExpr *NewComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right) {
    return new (region_) ComparisonOpExpr(pos, op, left, right);
  }

  CallExpr *NewCallExpr(Expr *fun, util::RegionVector<Expr *> &&args) {
    return new (region_) CallExpr(fun, std::move(args));
  }

  LitExpr *NewNilLiteral(const SourcePosition &pos) { return new (region_) LitExpr(pos); }

  LitExpr *NewBoolLiteral(const SourcePosition &pos, bool val) { return new (region_) LitExpr(pos, val); }

  LitExpr *NewIntLiteral(const SourcePosition &pos, i32 num) { return new (region_) LitExpr(pos, num); }

  LitExpr *NewFloatLiteral(const SourcePosition &pos, f32 num) { return new (region_) LitExpr(pos, num); }

  LitExpr *NewStringLiteral(const SourcePosition &pos, Identifier str) {
    return new (region_) LitExpr(pos, LitExpr::LitKind::String, str);
  }

  FunctionLitExpr *NewFunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body) {
    return new (region_) FunctionLitExpr(type_repr, body);
  }

  UnaryOpExpr *NewUnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr) {
    return new (region_) UnaryOpExpr(pos, op, expr);
  }

  IdentifierExpr *NewIdentifierExpr(const SourcePosition &pos, Identifier name) {
    return new (region_) IdentifierExpr(pos, name);
  }

  ImplicitCastExpr *NewImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind, Type *target_type, Expr *input) {
    return new (region_) ImplicitCastExpr(pos, cast_kind, target_type, input);
  }

  IndexExpr *NewIndexExpr(const SourcePosition &pos, Expr *object, Expr *index) {
    return new (region_) IndexExpr(pos, object, index);
  }

  MemberExpr *NewMemberExpr(const SourcePosition &pos, Expr *object, Expr *member) {
    return new (region_) MemberExpr(pos, object, member);
  }

  ArrayTypeRepr *NewArrayType(const SourcePosition &pos, Expr *len, Expr *elem_type) {
    return new (region_) ArrayTypeRepr(pos, len, elem_type);
  }

  FieldDecl *NewFieldDecl(const SourcePosition &pos, Identifier name, Expr *type_repr) {
    return new (region_) FieldDecl(pos, name, type_repr);
  }

  FunctionTypeRepr *NewFunctionType(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&params, Expr *ret) {
    return new (region_) FunctionTypeRepr(pos, std::move(params), ret);
  }

  PointerTypeRepr *NewPointerType(const SourcePosition &pos, Expr *base) {
    return new (region_) PointerTypeRepr(pos, base);
  }

  StructTypeRepr *NewStructType(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&fields) {
    return new (region_) StructTypeRepr(pos, std::move(fields));
  }

  MapTypeRepr *NewMapType(const SourcePosition &pos, Expr *key_type, Expr *val_type) {
    return new (region_) MapTypeRepr(pos, key_type, val_type);
  }

 private:
  util::Region *region_;
};

}  // namespace tpl::ast
