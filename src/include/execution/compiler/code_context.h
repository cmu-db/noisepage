#pragma once

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/macros.h"
#include "execution/compiler/codegen.h"

namespace tpl::ast {
class FunctionDecl;
}

namespace tpl::compiler {

class FunctionBuilder;
class CodeGen;

class CodeContext {
  friend class CodeGen;
 public:
  explicit CodeContext(util::Region *region);

  CodeGen &GetCodeGen() { return codeGen_; }

  DISALLOW_COPY_AND_MOVE(CodeContext);


  void SetCurrentFunction(FunctionBuilder *fn) { curr_fn_ = fn; }
  FunctionBuilder *GetCurrentFunction() const { return curr_fn_; }

  ast::Identifier NewIdentifier() {
    auto *s = new std::string(std::to_string(uniq_id_++));
    return ast::Identifier(s->c_str());
  }

 private:
  util::Region *region_;
  sema::ErrorReporter error_reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory ast_factory_;
  FunctionBuilder *curr_fn_;
  CodeGen codeGen_;

  ast::Expr *nil_type_;
  ast::Expr *bool_type_;
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

  u64 uniq_id_;
};

}