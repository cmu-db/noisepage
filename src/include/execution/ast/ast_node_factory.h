#pragma once

#include <utility>

#include "execution/ast/ast.h"
#include "execution/util/region.h"

namespace noisepage::execution::ast {

/**
 * A factory for AST nodes. This factory uses a region allocator to quickly
 * allocate AST nodes during parsing. The assumption here is that the nodes are
 * only required during parsing and are thrown away after code generation, hence
 * require quick deallocation as well, thus the use of a region.
 */
class AstNodeFactory {
 public:
  /**
   * Constructor
   * @param region region to use for allocation
   */
  explicit AstNodeFactory(util::Region *region) : region_(region) {}

  /**
   * Prevent copy and move
   */
  DISALLOW_COPY_AND_MOVE(AstNodeFactory);

  /**
   * @param pos source position
   * @param declarations list of top level declarations
   * @return created File node.
   */
  File *NewFile(const SourcePosition &pos, util::RegionVector<Decl *> &&declarations) {
    return new (region_) File(pos, std::move(declarations));
  }

  /**
   * @param pos source position
   * @param name function name
   * @param fun function literal (params, return type, body)
   * @return created FunctionDecl node.
   */
  FunctionDecl *NewFunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *fun) {
    return new (region_) FunctionDecl(pos, name, fun);
  }

  /**
   * @param pos source position
   * @param name struct name
   * @param type_repr struct type (field names and types)
   * @return created StructDecl node.
   */
  StructDecl *NewStructDecl(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr) {
    return new (region_) StructDecl(pos, name, type_repr);
  }

  /**
   * @param pos source position
   * @param name variable name
   * @param type_repr variable type
   * @param init initializer
   * @return created VariableDecl node
   */
  VariableDecl *NewVariableDecl(const SourcePosition &pos, Identifier name, Expr *type_repr, Expr *init) {
    return new (region_) VariableDecl(pos, name, type_repr, init);
  }

  /**
   * @param start_pos beginning source position
   * @param end_pos end source position
   * @param statements list of statements within the block
   * @return created BlockStmt node.
   */
  BlockStmt *NewBlockStmt(const SourcePosition &start_pos, const SourcePosition &end_pos,
                          util::RegionVector<Stmt *> &&statements) {
    return new (region_) BlockStmt(start_pos, end_pos, std::move(statements));
  }

  /**
   * @param decl the new declaration
   * @return created DeclStmt node
   */
  DeclStmt *NewDeclStmt(Decl *decl) { return new (region_) DeclStmt(decl); }

  /**
   * @param pos source position
   * @param dest assignment destination
   * @param src assignment source
   * @return created AssignmentStmt node
   */
  AssignmentStmt *NewAssignmentStmt(const SourcePosition &pos, Expr *dest, Expr *src) {
    return new (region_) AssignmentStmt(pos, dest, src);
  }

  /**
   * @param expression expression of the statement
   * @return created ExpressionStmt node
   */
  ExpressionStmt *NewExpressionStmt(Expr *expression) { return new (region_) ExpressionStmt(expression); }

  /**
   * @param pos source position
   * @param init loop initializer
   * @param cond loop condition
   * @param next loop update
   * @param body loop body
   * @return created ForStmt node
   */
  ForStmt *NewForStmt(const SourcePosition &pos, Stmt *init, Expr *cond, Stmt *next, BlockStmt *body) {
    return new (region_) ForStmt(pos, init, cond, next, body);
  }

  /**
   * @param pos source position
   * @param target variable in which tuples are stored
   * @param iter container (e.g. sql table) to iterate over
   * @param body loop body
   * @return created ForInStmt node
   */
  ForInStmt *NewForInStmt(const SourcePosition &pos, Expr *target, Expr *iter, BlockStmt *body) {
    return new (region_) ForInStmt(pos, target, iter, body);
  }

  /**
   * @param pos source position
   * @param cond if condition
   * @param then_stmt them statement
   * @param else_stmt else statement
   * @return created IfStmt node
   */
  IfStmt *NewIfStmt(const SourcePosition &pos, Expr *cond, BlockStmt *then_stmt, Stmt *else_stmt) {
    return new (region_) IfStmt(pos, cond, then_stmt, else_stmt);
  }

  /**
   * @param pos source position
   * @param ret returned expression
   * @return created ReturnStmt node
   */
  ReturnStmt *NewReturnStmt(const SourcePosition &pos, Expr *ret) { return new (region_) ReturnStmt(pos, ret); }

  /**
   * @param pos source position
   * @return created BadExpr
   */
  BadExpr *NewBadExpr(const SourcePosition &pos) { return new (region_) BadExpr(pos); }

  /**
   * @param pos source position
   * @param op binary operator
   * @param left lhs of the operator
   * @param right rhs of the operator
   * @return created BinaryOpExpr node
   */
  BinaryOpExpr *NewBinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right) {
    return new (region_) BinaryOpExpr(pos, op, left, right);
  }

  /**
   * @param pos source position
   * @param op comparision operator
   * @param left lhs of the operator
   * @param right rhs of the operator
   * @return created ComparisonOpExpr node
   */
  ComparisonOpExpr *NewComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right) {
    return new (region_) ComparisonOpExpr(pos, op, left, right);
  }

  /**
   * @param fun function being called
   * @param args arguments to the function
   * @return created CallExpr node
   */
  CallExpr *NewCallExpr(Expr *fun, util::RegionVector<Expr *> &&args) {
    return new (region_) CallExpr(fun, std::move(args));
  }

  /**
   * @param fun function being called
   * @param args arguments to the function
   * @return created CallExpr node
   */
  CallExpr *NewBuiltinCallExpr(Expr *fun, util::RegionVector<Expr *> &&args) {
    return new (region_) CallExpr(fun, std::move(args), CallExpr::CallKind::Builtin);
  }

  /**
   * @param pos source position
   * @return create nil LitExpr node
   */
  LitExpr *NewNilLiteral(const SourcePosition &pos) { return new (region_) LitExpr(pos); }

  /**
   * @param pos source position
   * @param val boolean value
   * @return created bool LitExpr node.
   */
  LitExpr *NewBoolLiteral(const SourcePosition &pos, bool val) { return new (region_) LitExpr(pos, val); }

  /**
   * @param pos source position
   * @param num integer value
   * @return created integer LitExpr node.
   */
  LitExpr *NewIntLiteral(const SourcePosition &pos, int64_t num) { return new (region_) LitExpr(pos, num); }

  /**
   * @param pos source position
   * @param num float value
   * @return created float LitExpr node
   */
  LitExpr *NewFloatLiteral(const SourcePosition &pos, double num) { return new (region_) LitExpr(pos, num); }

  /**
   * @param pos source position
   * @param str string value
   * @return created string LitExpr node
   */
  LitExpr *NewStringLiteral(const SourcePosition &pos, Identifier str) { return new (region_) LitExpr(pos, str); }

  /**
   * @param type_repr function type repr (param types, return type)
   * @param body body of the function
   * @return created FunctionLitExpr node
   */
  FunctionLitExpr *NewFunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body) {
    return new (region_) FunctionLitExpr(type_repr, body);
  }

  /**
   * @param pos source position
   * @param op unary operator
   * @param expr operand
   * @return created UnaryOpExpr node
   */
  UnaryOpExpr *NewUnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr) {
    return new (region_) UnaryOpExpr(pos, op, expr);
  }

  /**
   * @param pos source position
   * @param name identifier name
   * @return created IdentiferExpr node
   */
  IdentifierExpr *NewIdentifierExpr(const SourcePosition &pos, Identifier name) {
    return new (region_) IdentifierExpr(pos, name);
  }

  /**
   * @param pos source position
   * @param cast_kind cast kind
   * @param target_type type of the resulting expression
   * @param input input of the cast
   * @return created ImplicitCastExpr node
   */
  ImplicitCastExpr *NewImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind, Type *target_type, Expr *input) {
    return new (region_) ImplicitCastExpr(pos, cast_kind, target_type, input);
  }

  /**
   * @param pos source position
   * @param object indexed object
   * @param index accessed index
   * @return created IndexExpr node
   */
  IndexExpr *NewIndexExpr(const SourcePosition &pos, Expr *object, Expr *index) {
    return new (region_) IndexExpr(pos, object, index);
  }

  /**
   * @param pos source position
   * @param object accessed object
   * @param member accessed member
   * @return created MemberExpr node
   */
  MemberExpr *NewMemberExpr(const SourcePosition &pos, Expr *object, Expr *member) {
    return new (region_) MemberExpr(pos, object, member);
  }

  /**
   * @param pos source position
   * @param len array length
   * @param elem_type element type
   * @return created ArrayTypeRepr node
   */
  ArrayTypeRepr *NewArrayType(const SourcePosition &pos, Expr *len, Expr *elem_type) {
    return new (region_) ArrayTypeRepr(pos, len, elem_type);
  }

  /**
   * @param pos source position
   * @param name field name
   * @param type_repr field type repr
   * @return created FieldDecl node
   */
  FieldDecl *NewFieldDecl(const SourcePosition &pos, Identifier name, Expr *type_repr) {
    return new (region_) FieldDecl(pos, name, type_repr);
  }

  /**
   * @param pos source position
   * @param params list of parameters
   * @param ret return type
   * @return created FunctionTypeRepr node
   */
  FunctionTypeRepr *NewFunctionType(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&params, Expr *ret) {
    return new (region_) FunctionTypeRepr(pos, std::move(params), ret);
  }

  /**
   * @param pos source position
   * @param base pointee type
   * @return created PointerTypeRepr node
   */
  PointerTypeRepr *NewPointerType(const SourcePosition &pos, Expr *base) {
    return new (region_) PointerTypeRepr(pos, base);
  }

  /**
   * @param pos source position
   * @param fields list of struct fields
   * @return created StructTypeRepr node
   */
  StructTypeRepr *NewStructType(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&fields) {
    return new (region_) StructTypeRepr(pos, std::move(fields));
  }

  /**
   * @param pos source position
   * @param key_type key type
   * @param val_type value type
   * @return created MapTypeRepr node
   */
  MapTypeRepr *NewMapType(const SourcePosition &pos, Expr *key_type, Expr *val_type) {
    return new (region_) MapTypeRepr(pos, key_type, val_type);
  }

 private:
  util::Region *region_;
};

}  // namespace noisepage::execution::ast
