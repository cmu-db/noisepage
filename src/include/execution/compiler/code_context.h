#pragma once

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/macros.h"

namespace tpl::ast {
class FunctionDecl;
}

namespace tpl::compiler {
class FunctionBuilder;

class CodeContext {
  friend class CodeGen;
 public:
  explicit CodeContext(util::Region *region);
  DISALLOW_COPY_AND_MOVE(CodeContext);

  void SetCurrentFunction(FunctionBuilder *fn) { curr_fn_ = fn; }
  FunctionBuilder *GetCurrentFunction() const { return curr_fn_; }

  void AddTopDecl(ast::Decl *decl) {
    decls_.emplace_back(decl);
  }

  ast::File *CompileToFile(CodeGen *codegen) {
    return (*codegen)->NewFile(DUMMY_POS, std::move(decls_));
  }

  sema::ErrorReporter *GetReporter() {
    return &error_reporter_;
  }

  ast::Context *GetAstContext() {
    return &ast_ctx_;
  }

 private:
  util::Region *region_;
  sema::ErrorReporter error_reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory ast_factory_;
  FunctionBuilder *curr_fn_;
  util::RegionVector<ast::Decl *> decls_;

  ast::Expr *nil_type_;
  ast::Expr *bool_type_;
  ast::Expr *int_type_;
  ast::Expr *i8_type_;
  ast::Expr *i16_type_;
  ast::Expr *i32_type_;
  ast::Expr *i64_type_;
  ast::Expr *i128_type_;
  ast::Expr *u8_type_;
  ast::Expr *u16_type_;
  ast::Expr *u32_type_;
  ast::Expr *u64_type_;
  ast::Expr *u128_type_;
  ast::Expr *f32_type_;
  ast::Expr *f64_type_;
};

}