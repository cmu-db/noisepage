
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/code_context.h"

#include "execution/compiler/codegen.h"

namespace tpl::compiler {

CodeGen::CodeGen(CodeContext *ctx) : id_count_(0), ctx_(ctx), factory_(&ctx_->ast_factory_) {}

util::Region *CodeGen::GetRegion() { return ctx_->region_; }

FunctionBuilder *CodeGen::GetCurrentFunction() { return ctx_->GetCurrentFunction(); }

ast::BlockStmt *CodeGen::EmptyBlock() const {
  util::RegionVector<ast::Stmt *> stmts(ctx_->region_);
  return factory_->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
}

ast::Identifier CodeGen::NewIdentifier() {
  // Use the custom allocator because the id will outlive the std::string.
  std::string id = "id" + std::to_string(id_count_++);
  char* id_str = GetRegion()->AllocateArray<char>(id.size() + 1);
  std::memcpy(id_str, id.c_str(), id.size() + 1);
  return ctx_->GetAstContext()->GetIdentifier(id_str);
}

ast::Stmt *CodeGen::Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr*> &&args) {
  return factory_->NewExpressionStmt(factory_->NewCallExpr(fn->function(), std::move(args)));
}

ast::Expr *CodeGen::Ty_Nil() const { return ctx_->nil_type_; }
ast::Expr *CodeGen::Ty_Bool() const { return ctx_->bool_type_; }
ast::Expr *CodeGen::Ty_Int8() const { return ctx_->i8_type_; }
ast::Expr *CodeGen::Ty_Int16() const { return ctx_->i16_type_; }
ast::Expr *CodeGen::Ty_Int32() const { return ctx_->i32_type_; }
ast::Expr *CodeGen::Ty_Int64() const { return ctx_->i64_type_; }
ast::Expr *CodeGen::Ty_Int128() const { return ctx_->i128_type_; }
ast::Expr *CodeGen::Ty_UInt8() const { return ctx_->u8_type_; }
ast::Expr *CodeGen::Ty_UInt16() const { return ctx_->u16_type_; }
ast::Expr *CodeGen::Ty_UInt32() const { return ctx_->u32_type_; }
ast::Expr *CodeGen::Ty_UInt64() const { return ctx_->u64_type_; }
ast::Expr *CodeGen::Ty_UInt128() const { return ctx_->u128_type_; }
ast::Expr *CodeGen::Ty_Float32() const { return ctx_->f32_type_; }
ast::Expr *CodeGen::Ty_Float64() const { return ctx_->f64_type_; }


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

} // namespace tpl::compiler