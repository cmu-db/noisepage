
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
    return ast::Identifier(std::to_string(id_count_++).c_str());
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


} // namespace tpl::compiler