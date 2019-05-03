#pragma once

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/macros.h"
#include "execution/util/region.h"

namespace tpl::compiler {

class CodeContext;

/**
 * CodeGen API is responsible for generating all TPL AST.
 */
class CodeGen {
 public:
  explicit CodeGen(CodeContext *ctx) : ctx_(ctx), factory_(&ctx_->ast_factory_) {}

  DISALLOW_COPY_AND_MOVE(CodeGen);

  ast::FunctionDecl *GetFunction(std::string name, ast::Type *ret_type, util::RegionVector<ast::FieldDecl *> args,
                                 CodeBlock &block) {
    auto nameIdentifier = ast::Identifier(name.data());
    auto retIdentifier = factory_->NewIdentifierExpr(DUMMY_POS, ast::Identifier(ret_type->ToString().data()));
    return factory_->NewFunctionDecl(
        DUMMY_POS, nameIdentifier,
        factory_->NewFunctionLitExpr(factory_->NewFunctionType(DUMMY_POS, std::move(args), retIdentifier),
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

  ast::Stmt *Return(ast::Expr *val) const { return factory_->NewReturnStmt(DUMMY_POS, val); }

  ast::Type *Ty_Nil() const { return ctx_->nil_type_; }
  ast::Type *Ty_Bool() const { return ctx_->bool_type_; }
  ast::Type *Ty_Int8() const { return ctx_->i8_type_; }
  ast::Type *Ty_Int16() const { return ctx_->i16_type_; }
  ast::Type *Ty_Int32() const { return ctx_->i32_type_; }
  ast::Type *Ty_Int64() const { return ctx_->i64_type_; }
  ast::Type *Ty_Int128() const { return ctx_->i128_type_; }
  ast::Type *Ty_UInt8() const { return ctx_->u8_type_; }
  ast::Type *Ty_UInt16() const { return ctx_->u16_type_; }
  ast::Type *Ty_UInt32() const { return ctx_->u32_type_; }
  ast::Type *Ty_UInt64() const { return ctx_->u64_type_; }
  ast::Type *Ty_UInt128() const { return ctx_->u128_type_; }
  ast::Type *Ty_Float32() const { return ctx_->f32_type_; }
  ast::Type *Ty_Float64() const { return ctx_->f64_type_; }

 private:
  CodeContext *ctx_;
  ast::AstNodeFactory *factory_;
};

}  // namespace tpl::compiler