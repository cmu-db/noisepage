#pragma once

#include <string>
#include <utility>

#include "execution/ast/identifier.h"
#include "execution/compiler/ast_fwd.h"
#include "execution/util/region_containers.h"

namespace noisepage::execution::compiler {

class CodeGen;

/**
 * Helper class to build TPL functions.
 */
class FunctionBuilder {
  friend class If;
  friend class Loop;

 public:
  /**
   * Create a builder for a function with the provided name, return type, and arguments.
   * @param codegen The code generation instance.
   * @param name The name of the function.
   * @param params The parameters to the function.
   * @param ret_type The return type representation of the function.
   */
  FunctionBuilder(CodeGen *codegen, ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&params,
                  ast::Expr *ret_type);

  /**
   * Destructor.
   */
  ~FunctionBuilder();

  /**
   * @return A reference to a function parameter by its ordinal position.
   */
  ast::Expr *GetParameterByPosition(uint32_t param_idx);

  /**
   * Append a statement to the list of statements in this function.
   * @param stmt The statement to append.
   */
  void Append(ast::Stmt *stmt);

  /**
   * Append an expression as a statement to the list of statements in this function.
   * @param expr The expression to append as a statement.
   */
  void Append(ast::Expr *expr);

  /**
   * Append a variable declaration as a statement to the list of statements in this function.
   * @param decl The declaration to append to the statement.
   */
  void Append(ast::VariableDecl *decl);

  /**
   * Finish constructing the function.
   * @param ret The value to return from the function. Use a null pointer to return nothing.
   * @return The build function declaration.
   */
  ast::FunctionDecl *Finish(ast::Expr *ret = nullptr);

  /**
   * @return The final constructed function; null if the builder hasn't been constructed through
   *         FunctionBuilder::Finish().
   */
  ast::FunctionDecl *GetConstructedFunction() const { return decl_; }

  /**
   * @return The code generator instance.
   */
  CodeGen *GetCodeGen() const { return codegen_; }

 private:
  // The code generation instance.
  CodeGen *codegen_;
  // The function's name.
  ast::Identifier name_;
  // The function's arguments.
  util::RegionVector<ast::FieldDecl *> params_;
  // The return type of the function.
  ast::Expr *ret_type_;
  // The start and stop position of statements in the function.
  SourcePosition start_;
  // The list of generated statements making up the function.
  ast::BlockStmt *statements_;
  // The cached function declaration. Constructed once in Finish().
  ast::FunctionDecl *decl_;
};

}  // namespace noisepage::execution::compiler
