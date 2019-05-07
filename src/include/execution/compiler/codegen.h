#pragma once

#include "execution/compiler/code_context.h"
#include "execution/util/macros.h"

namespace tpl::ast {
class Stmt;
class Expr;
}

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeGen {
 private:
  friend class FunctionBuilder;
  friend class QueryState;
  util::Region *GetRegion();
  CodeContext *GetCodeContext() { return ctx_; }

 public:
  explicit CodeGen(CodeContext *ctx);

  ast::AstNodeFactory *operator->() { return factory_; }

  FunctionBuilder *GetCurrentFunction();

  DISALLOW_COPY_AND_MOVE(CodeGen);

  /*ast::FunctionDecl *GetFunction(std::string name, ast::Expr *ret_type, util::RegionVector<ast::FieldDecl *> args,
                                 CodeBlock &block) {
    auto nameIdentifier = ast::Identifier(name.data());
    return factory_->NewFunctionDecl(
        DUMMY_POS, nameIdentifier,
        factory_->NewFunctionLitExpr(factory_->NewFunctionType(DUMMY_POS, std::move(args), ret_type),
                                     block.Compile(factory_, ctx_->region_)));
  }

  ast::BlockStmt *Compile(CodeBlock &block) { block.Compile(factory_, ctx_->region_); }

  ast::Expr *Val_Nil() const { return factory_->NewNilLiteral(DUMMY_POS); }

  ast::Expr *Val_Bool(bool b) const { return factory_->NewBoolLiteral(DUMMY_POS, b); }

  ast::Expr *Val_Int(i32 n) const { return factory_->NewIntLiteral(DUMMY_POS, n); }

  ast::Expr *Val_Float(f32 f) const { return factory_->NewFloatLiteral(DUMMY_POS, f); }

  ast::Stmt *ForInStmt(ast::Expr *target, ast::Expr *table_name, ast::Attributes *attributes,
                       ast::BlockStmt *body) const {
    return factory_->NewForInStmt(DUMMY_POS, target, table_name, attributes, body);
  }

  ast::Stmt *AssignStmt(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewAssignmentStmt(DUMMY_POS, left, right);
  }

  ast::Expr *Add(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::PLUS, left, right);
  }

  ast::Expr *Subtract(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::MINUS, left, right);
  }

  ast::Expr *Multiply(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::STAR, left, right);
  }

  ast::Expr *Divide(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::SLASH, left, right);
  }

  ast::Expr *Modulo(ast::Expr *left, ast::Expr *right) const {
    return factory_->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::PERCENT, left, right);
  }

  ast::Stmt *Return(ast::Expr *val) const { return factory_->NewReturnStmt(DUMMY_POS, val); }*/

  ast::Expr *Ty_Nil() const;
  ast::Expr *Ty_Bool() const;
  ast::Expr *Ty_Int8() const;
  ast::Expr *Ty_Int16() const;
  ast::Expr *Ty_Int32() const;
  ast::Expr *Ty_Int64() const;
  ast::Expr *Ty_Int128() const;
  ast::Expr *Ty_UInt8() const;
  ast::Expr *Ty_UInt16() const;
  ast::Expr *Ty_UInt32() const;
  ast::Expr *Ty_UInt64() const;
  ast::Expr *Ty_UInt128() const;
  ast::Expr *Ty_Float32() const;
  ast::Expr *Ty_Float64() const;

  ast::BlockStmt *EmptyBlock() const;

  ast::Identifier NewIdentifier();

  ast::Stmt *Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr*> &&args);

 private:
  static constexpr SourcePosition DUMMY_POS{0, 0};

  u64 id_count_;
  CodeContext *ctx_;
  ast::AstNodeFactory *factory_;


};

}