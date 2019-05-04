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

class CodeContext;

class CodeGen {
 private:
  friend class FunctionBuilder;
  friend class QueryState;
  util::Region *GetRegion() { return ctx_->region_; }
  CodeContext *GetCodeContext() { return ctx_; }

 public:
  explicit CodeGen(CodeContext *ctx) : ctx_(ctx), factory_(&ctx_->ast_factory_) {}

  ast::AstNodeFactory *operator->() { return factory_; }

  DISALLOW_COPY_AND_MOVE(CodeGen);

  ast::Expr *Ty_Nil() const { return ctx_->nil_type_; }
  ast::Expr *Ty_Bool() const { return ctx_->bool_type_; }
  ast::Expr *Ty_Int8() const { return ctx_->i8_type_; }
  ast::Expr *Ty_Int16() const { return ctx_->i16_type_; }
  ast::Expr *Ty_Int32() const { return ctx_->i32_type_; }
  ast::Expr *Ty_Int64() const { return ctx_->i64_type_; }
  ast::Expr *Ty_Int128() const { return ctx_->i128_type_; }
  ast::Expr *Ty_UInt8() const { return ctx_->u8_type_; }
  ast::Expr *Ty_UInt16() const { return ctx_->u16_type_; }
  ast::Expr *Ty_UInt32() const { return ctx_->u32_type_; }
  ast::Expr *Ty_UInt64() const { return ctx_->u64_type_; }
  ast::Expr *Ty_UInt128() const { return ctx_->u128_type_; }
  ast::Expr *Ty_Float32() const { return ctx_->f32_type_; }
  ast::Expr *Ty_Float64() const { return ctx_->f64_type_; }

  ast::BlockStmt *EmptyBlock() const {
    util::RegionVector<ast::Stmt *> stmts(ctx_->region_);
    return factory_->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
  }

 private:
  static constexpr SourcePosition DUMMY_POS{0, 0};

  CodeContext *ctx_;
  ast::AstNodeFactory *factory_;


};

}