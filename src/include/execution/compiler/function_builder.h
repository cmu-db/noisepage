#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/macros.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution::compiler {

class CodeGen;

/** Enumerates the function types */
enum class FunctionType { FUNCTION, CLOSURE };

/**
 * Helper class to build TPL functions.
 */
class FunctionBuilder {
  friend class If;
  friend class Loop;

 public:
  /**
   * Construct a new FunctionBuilder instance for a "vanilla" function.
   * @param codegen The code generation instance
   * @param name The function name
   * @param params The function parameters
   * @param return_type The return type representation of the function
   */
  FunctionBuilder(CodeGen *codegen, ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&params,
                  ast::Expr *return_type);

  /**
   * Construct a new FunctionBuilder instance for a closure.
   * @param codegen The code generation instance
   * @param params The function parameters
   * @param captures The function captures
   * @param return_type The return type representation of the function
   */
  FunctionBuilder(CodeGen *codegen, util::RegionVector<ast::FieldDecl *> &&params,
                  util::RegionVector<ast::Expr *> &&captures, ast::Expr *return_type);

  /** Destructor; invokes FunctionBuilder::Finish() */
  ~FunctionBuilder();

  /** @return The arity of the function */
  std::size_t GetParameterCount() const { return params_.size(); }

  /** @return A reference to a function parameter by its ordinal position */
  ast::Expr *GetParameterByPosition(std::size_t param_idx);

  /** @return The expression representation of the parameters to the function */
  std::vector<ast::Expr *> GetParameters() const;

  /**
   * Append a statement to the list of statements in this function.
   * @param stmt The statement to append
   */
  void Append(ast::Stmt *stmt);

  /**
   * Append an expression as a statement to the list of statements in this function.
   * @param expr The expression to append as a statement
   */
  void Append(ast::Expr *expr);

  /**
   * Append a variable declaration as a statement to the list of statements in this function.
   * @param decl The declaration to append to the statement
   */
  void Append(ast::VariableDecl *decl);

  /**
   * Finish construction of the function.
   * @param ret The function return value; use `nullptr` for `nil` return
   * @return The finished declaration
   */
  ast::FunctionDecl *Finish(ast::Expr *ret = nullptr);

  /**
   * Finish construction of the closure.
   * @param ret The function return value; use `nullptr` for `nil` return
   * @return The finished expression
   */
  ast::LambdaExpr *FinishClosure(ast::Expr *ret = nullptr);

  /** @return The final constructed function */
  ast::FunctionDecl *GetFinishedFunction() const {
    NOISEPAGE_ASSERT(type_ == FunctionType::FUNCTION, "Attempt to get function from non-function-type builder");
    return std::get<ast::FunctionDecl *>(decl_);
  }

  /** @return The final constructed closure */
  ast::LambdaExpr *GetFinishedClosure() const {
    NOISEPAGE_ASSERT(type_ == FunctionType::CLOSURE, "Attempt to get closure from non-closure-type builder");
    return std::get<ast::LambdaExpr *>(decl_);
  }

  /** @return The code generator instance. */
  CodeGen *GetCodeGen() const { return codegen_; }

  /** @return `true` if the function is a closure, `false` otherwise */
  bool IsClosure() const { return type_ == FunctionType::CLOSURE; }

 private:
  /** The type of the function */
  FunctionType type_;
  /** The code generation instance */
  CodeGen *codegen_;
  /** The function's name */
  ast::Identifier name_;
  /** The function's arguments */
  util::RegionVector<ast::FieldDecl *> params_;
  /** The captures for the closure (if applicable) */
  util::RegionVector<ast::Expr *> captures_;
  /** The return type of the function */
  ast::Expr *return_type_;
  /** The start and stop position of statements in the function */
  SourcePosition start_;
  /** The list of generated statements making up the function */
  ast::BlockStmt *statements_;
  /** The cached, completed function; constructed once in Finish() */
  std::variant<ast::FunctionDecl *, ast::LambdaExpr *> decl_;
};

}  // namespace noisepage::execution::compiler
