#pragma once

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/compiler/code_block.h"
#include "execution/sema/error_reporter.h"

namespace tpl::ast {
class Type;
}

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class FunctionBuilder;

class CodeContext {
  friend class CodeGen;
  friend class FunctionBuilder;

 public:
  explicit CodeContext(util::Region *region);

  CodeBlock &GetCodeBlock() { return codeBlock_; }

  void FinishFunction(ast::FunctionDecl *fn_decl) { decls_.push_back(fn_decl); }

 private:
  void SetCurrentFunction(FunctionBuilder *fn) { curr_fn_ = fn; }

 private:
  util::Region *region_;
  sema::ErrorReporter reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory ast_factory_;
  CodeBlock codeBlock_;
  util::RegionVector<ast::Decl *> decls_;

  FunctionBuilder *curr_fn_;

  ast::Type *nil_type_;
  ast::Type *bool_type_;
  ast::Type *i8_type_;
  ast::Type *i16_type_;
  ast::Type *i32_type_;
  ast::Type *i64_type_;
  ast::Type *i128_type_;
  ast::Type *u8_type_;
  ast::Type *u16_type_;
  ast::Type *u32_type_;
  ast::Type *u64_type_;
  ast::Type *u128_type_;
  ast::Type *f32_type_;
  ast::Type *f64_type_;
};

}  // namespace tpl::compiler