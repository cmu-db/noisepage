#pragma once

#include <utility>
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

/**
 * Stores information needed during code generation.
 */
class CodeContext {
  friend class CodeGen;

 public:
  /**
   * Constructor
   * @param region region to use for allocation
   */
  explicit CodeContext(util::Region *region);

  /// Preven copy and move
  DISALLOW_COPY_AND_MOVE(CodeContext);

  /**
   * Sets the current function being generated
   * @param fn new current function
   */
  void SetCurrentFunction(FunctionBuilder *fn) { curr_fn_ = fn; }

  /**
   * @return the current function
   */
  FunctionBuilder *GetCurrentFunction() const { return curr_fn_; }

  /**
   * Add a top level declaration
   * @param decl new declaration to add
   */
  void AddTopDecl(ast::Decl *decl) { decls_.emplace_back(decl); }

  /**
   * Finalize compilation by creating a File node
   * @param codegen code generator to use
   * @return the created File node
   */
  ast::File *CompileToFile(CodeGen *codegen) { return (*codegen)->NewFile(DUMMY_POS, std::move(decls_)); }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter *GetReporter() { return &error_reporter_; }

  /**
   * @return the ast context
   */
  ast::Context *GetAstContext() { return &ast_ctx_; }

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

}  // namespace tpl::compiler
