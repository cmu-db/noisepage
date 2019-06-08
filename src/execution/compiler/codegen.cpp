#include "execution/compiler/codegen.h"

#include <string>
#include <utility>
#include "execution/compiler/code_context.h"
#include "execution/compiler/function_builder.h"
#include "type/transient_value_peeker.h"

namespace tpl::compiler {

CodeGen::CodeGen(CodeContext *ctx) : id_count_(0), ctx_(ctx), factory_(&ctx_->ast_factory_) {}

util::Region *CodeGen::GetRegion() { return ctx_->region_; }

FunctionBuilder *CodeGen::GetCurrentFunction() { return ctx_->GetCurrentFunction(); }

ast::BlockStmt *CodeGen::EmptyBlock() const {
  util::RegionVector<ast::Stmt *> stmts(ctx_->region_);
  return factory_->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
}

ast::Identifier CodeGen::NewIdentifier() {
  return NewIdentifer("id");
}

ast::Identifier CodeGen::NewIdentifer(const std::string &prefix) {
  // Use the custom allocator because the id will outlive the std::string.
  std::string id = prefix + std::to_string(id_count_++);
  auto *id_str = GetRegion()->AllocateArray<char>(id.size() + 1);
  std::memcpy(id_str, id.c_str(), id.size() + 1);
  return ast_ctx_->GetIdentifier(id_str);

}

ast::Stmt *CodeGen::Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr *> &&args) {
  return factory_->NewExpressionStmt(factory_->NewCallExpr(fn->function(), std::move(args)));
}

ast::IdentifierExpr *CodeGen::BptrCast() {
  return factory_->NewIdentifierExpr(DUMMY_POS, ctx_->GetAstContext()->GetIdentifier(ptrCast));
}

ast::IdentifierExpr *CodeGen::BoutputAlloc() {
  return factory_->NewIdentifierExpr(DUMMY_POS, ctx_->GetAstContext()->GetIdentifier(outputAlloc));
}

ast::IdentifierExpr *CodeGen::BoutputAdvance() {
  return factory_->NewIdentifierExpr(DUMMY_POS, ctx_->GetAstContext()->GetIdentifier(outputAdvance));
}

ast::IdentifierExpr *CodeGen::BoutputFinalize() {
  return factory_->NewIdentifierExpr(DUMMY_POS, ctx_->GetAstContext()->GetIdentifier(outputFinalize));
}

ast::IdentifierExpr *CodeGen::Binsert() {
  return factory_->NewIdentifierExpr(DUMMY_POS, ctx_->GetAstContext()->GetIdentifier(insert));
}

ast::Expr *CodeGen::PeekValue(const terrier::type::TransientValue &transient_val) const {
  switch (transient_val.Type()) {
    case terrier::type::TypeId::TINYINT: {
      auto val = terrier::type::TransientValuePeeker::PeekTinyInt(transient_val);
      return factory_->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::SMALLINT: {
      auto val = terrier::type::TransientValuePeeker::PeekSmallInt(transient_val);
      return factory_->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::INTEGER: {
      auto val = terrier::type::TransientValuePeeker::PeekInteger(transient_val);
      return factory_->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BIGINT: {
      // TODO(WAN): the factory's IntLiteral only goes to i32
      auto val = terrier::type::TransientValuePeeker::PeekBigInt(transient_val);
      return factory_->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BOOLEAN: {
      auto val = terrier::type::TransientValuePeeker::PeekBoolean(transient_val);
      return factory_->NewBoolLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
    case terrier::type::TypeId::DECIMAL:
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
    default:
      // TODO(WAN): error out
      return nullptr;
  }
}

ast::Expr *CodeGen::TyConvert(terrier::type::TypeId type) const {
  switch (type) {
    case terrier::type::TypeId::TINYINT: {
      return TyInt8();
    }
    case terrier::type::TypeId::SMALLINT: {
      return TyInt16();
    }
    case terrier::type::TypeId::INTEGER: {
      return TyInt32();
    }
    case terrier::type::TypeId::BIGINT: {
      return TyInt64();
    }
    case terrier::type::TypeId::BOOLEAN: {
      return TyBool();
    }
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
    case terrier::type::TypeId::DECIMAL:
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
    default:
      // TODO(WAN): error out
      return nullptr;
  }
}

ast::Expr *CodeGen::TyNil() const { return ctx_->nil_type_; }
ast::Expr *CodeGen::TyBool() const { return ctx_->bool_type_; }
ast::Expr *CodeGen::TyInteger() const { return ctx_->int_type_; }
ast::Expr *CodeGen::TyInt8() const { return ctx_->i8_type_; }
ast::Expr *CodeGen::TyInt16() const { return ctx_->i16_type_; }
ast::Expr *CodeGen::TyInt32() const { return ctx_->i32_type_; }
ast::Expr *CodeGen::TyInt64() const { return ctx_->i64_type_; }
ast::Expr *CodeGen::TyInt128() const { return ctx_->i128_type_; }
ast::Expr *CodeGen::TyUInt8() const { return ctx_->u8_type_; }
ast::Expr *CodeGen::TyUInt16() const { return ctx_->u16_type_; }
ast::Expr *CodeGen::TyUInt32() const { return ctx_->u32_type_; }
ast::Expr *CodeGen::TyUInt64() const { return ctx_->u64_type_; }
ast::Expr *CodeGen::TyUInt128() const { return ctx_->u128_type_; }
ast::Expr *CodeGen::TyFloat32() const { return ctx_->f32_type_; }
ast::Expr *CodeGen::TyFloat64() const { return ctx_->f64_type_; }

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

}  // namespace tpl::compiler
