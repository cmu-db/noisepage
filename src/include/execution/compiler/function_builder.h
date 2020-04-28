#pragma once

#include <list>
#include <string>

#include "common/macros.h"
#include "execution/ast/ast.h"
#include "execution/ast/identifier.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region_containers.h"

namespace terrier {
namespace execution {
namespace compiler {
class CodeGen;
}  // namespace compiler
}  // namespace execution
}  // namespace terrier

namespace terrier::execution::compiler {

/**
 * Builder for functions.
 */
class FunctionBuilder {
 public:
  /**
   * constructor
   * @param codegen code generator to use
   * @param fn_name function name
   * @param fn_params function parameters.
   * @param fn_ret_type function return type
   */
  FunctionBuilder(CodeGen *codegen, ast::Identifier fn_name, util::RegionVector<ast::FieldDecl *> &&fn_params,
                  ast::Expr *fn_ret_type);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  /**
   * Appends a statement to the current block
   * @param stmt statement to append.
   */
  void Append(ast::Stmt *stmt) { blocks_.back()->AppendStmt(stmt); }

  /**
   * Begins an IfStmt
   * @param condition if condition
   */
  void StartIfStmt(ast::Expr *condition);

  /**
   * Begins a ForStmt
   * @param init
   * @param cond
   * @param next
   */
  void StartForStmt(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next);

  /**
   * Finish an if or a for statement.
   */
  void FinishBlockStmt();

  /**
   * @return finalized FunctionDecl
   */
  ast::FunctionDecl *Finish();

 private:
  CodeGen *codegen_;

  ast::Identifier fn_name_;
  util::RegionVector<ast::FieldDecl *> fn_params_;
  ast::Expr *fn_ret_type_;

  ast::BlockStmt *fn_body_;
  std::list<ast::BlockStmt *> blocks_;
  std::list<ast::Stmt *> final_stmts_;
};

}  // namespace terrier::execution::compiler
