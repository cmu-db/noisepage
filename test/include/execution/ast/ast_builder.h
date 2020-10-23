#pragma once

#include <string>
#include <utility>
#include <vector>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution::ast::test {

class TestAstBuilder {
 public:
  TestAstBuilder() : region_("test_ast_builder"), error_reporter_(&region_), ctx_(&region_, &error_reporter_) {}

  Context *Ctx() { return &ctx_; }

  sema::ErrorReporter *ErrorReporter() { return &error_reporter_; }

  Identifier Ident(const std::string &s) { return Ctx()->GetIdentifier(s); }

  Expr *IdentExpr(Identifier ident) { return GetNodeFactory()->NewIdentifierExpr(empty_, ident); }

  Expr *IdentExpr(const std::string &s) { return IdentExpr(Ident(s)); }

  Expr *BoolLit(bool b) { return GetNodeFactory()->NewBoolLiteral(empty_, b); }

  Expr *IntLit(int32_t i) { return GetNodeFactory()->NewIntLiteral(empty_, i); }

  Expr *FloatLit(float i) { return GetNodeFactory()->NewFloatLiteral(empty_, i); }

  template <parsing::Token::Type OP>
  Expr *BinOp(Expr *left, Expr *right) {
    return GetNodeFactory()->NewBinaryOpExpr(empty_, OP, left, right);
  }

  template <parsing::Token::Type OP>
  Expr *Cmp(Expr *left, Expr *right) {
    NOISEPAGE_ASSERT(parsing::Token::IsCompareOp(OP), "Not a comparison");
    return GetNodeFactory()->NewComparisonOpExpr(empty_, OP, left, right);
  }

  Expr *CmpEq(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::EQUAL_EQUAL>(left, right); }
  Expr *CmpNe(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::BANG_EQUAL>(left, right); }
  Expr *CmpLt(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::LESS>(left, right); }

  Expr *Field(Expr *obj, Expr *field) { return GetNodeFactory()->NewMemberExpr(empty_, obj, field); }

  VariableDecl *DeclVar(Identifier name, Expr *init) { return DeclVar(name, nullptr, init); }

  VariableDecl *DeclVar(Identifier name, Expr *type_repr, Expr *init) {
    return GetNodeFactory()->NewVariableDecl(empty_, name, type_repr, init);
  }

  FieldDecl *GenFieldDecl(Identifier name, ast::Expr *type_repr) {
    return GetNodeFactory()->NewFieldDecl(empty_, name, type_repr);
  }

  StructDecl *DeclStruct(Identifier name, std::initializer_list<ast::FieldDecl *> fields) {
    util::RegionVector<FieldDecl *> f(fields.begin(), fields.end(), Ctx()->GetRegion());
    ast::StructTypeRepr *type = GetNodeFactory()->NewStructType(empty_, std::move(f));
    return GetNodeFactory()->NewStructDecl(empty_, name, type);
  }

  Expr *DeclRef(Decl *decl) { return IdentExpr(decl->Name()); }

  Stmt *DeclStmt(Decl *decl) { return GetNodeFactory()->NewDeclStmt(decl); }

  Stmt *Block(std::initializer_list<Stmt *> stmts) {
    util::RegionVector<Stmt *> region_stmts(stmts.begin(), stmts.end(), Ctx()->GetRegion());
    return GetNodeFactory()->NewBlockStmt(empty_, empty_, std::move(region_stmts));
  }

  Stmt *ExprStmt(Expr *expr) { return GetNodeFactory()->NewExpressionStmt(expr); }

  Expr *PtrType(Expr *base) { return GetNodeFactory()->NewPointerType(empty_, base); }

  template <BuiltinType::Kind BUILTIN>
  Expr *BuiltinTypeRepr() {
    return IdentExpr(BuiltinType::Get(Ctx(), BUILTIN)->GetTplName());
  }

  Expr *PrimIntTypeRepr() { return BuiltinTypeRepr<BuiltinType::Int32>(); }
  Expr *PrimFloatTypeRepr() { return BuiltinTypeRepr<BuiltinType::Float32>(); }
  Expr *PrimBoolTypeRepr() { return BuiltinTypeRepr<BuiltinType::Bool>(); }

  Expr *IntegerSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Integer>(); }
  Expr *RealSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Real>(); }
  Expr *StringSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::StringVal>(); }

  Expr *ArrayTypeRepr(Expr *type) { return GetNodeFactory()->NewArrayType(empty_, nullptr, type); }

  Expr *ArrayIndex(Expr *arr, Expr *idx) { return GetNodeFactory()->NewIndexExpr(empty_, arr, idx); }

  template <Builtin BUILTIN, typename... Args>
  CallExpr *Call(Args... args) {
    auto fn = IdentExpr(Builtins::GetFunctionName(BUILTIN));
    auto call_args = util::RegionVector<Expr *>({std::forward<Args>(args)...}, Ctx()->GetRegion());
    return GetNodeFactory()->NewBuiltinCallExpr(fn, std::move(call_args));
  }

  File *GenFile(std::initializer_list<ast::Decl *> decls) {
    util::RegionVector<Decl *> d(decls.begin(), decls.end(), Ctx()->GetRegion());
    return GetNodeFactory()->NewFile(empty_, std::move(d));
  }

 private:
  AstNodeFactory *GetNodeFactory() { return Ctx()->GetNodeFactory(); }

 private:
  util::Region region_;
  sema::ErrorReporter error_reporter_;
  Context ctx_;
  SourcePosition empty_{0, 0};
};

}  // namespace noisepage::execution::ast::test
