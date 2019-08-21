#pragma once

#include <list>
#include <string>
#include "execution/ast/ast.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region_containers.h"

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
   * Appends a stmt to the current block
   * @param stmt sttm to append.
   */
  void Append(ast::Stmt *stmt) { blocks_.back()->AppendStmt(stmt); }

  /**
   * Appends a stmt after the current block.
   * This function is used by operators that need to cleanup an iterator or free some structures.
   * @param stmt stmt to append
   */
  void AppendAfter(ast::Stmt *stmt) {
    auto iter = blocks_.rbegin();
    iter++;
    (*iter)->AppendStmt(stmt);
  }

  /**
   * Begins an IfStmt
   * @param condition if condition
   */
  void StartIfStmt(ast::Expr *condition);

  /**
   * Finish a block statement.
   */
  void FinishBlockStmt();

  /**
   * Begins a ForStmt
   * @param init
   * @param cond
   * @param next
   */
  void StartForStmt(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next);

  /**
   * @return the code generator used
   */
  CodeGen *GetCodeGen() { return codegen_; }

  /**
   * Allows an operator to register a statement that is executed at the end of the pipeline
   * @param stmt statement to register
   */
  void RegisterFinalStmt(ast::Stmt *stmt) { final_stmts_.push_back(stmt); }

  /**
   * @return finalized FunctionDecl
   */
  ast::FunctionDecl *Finish();

 private:
  CodeGen *codegen_;
  FunctionBuilder *prev_fn_;

  ast::Identifier fn_name_;
  util::RegionVector<ast::FieldDecl *> fn_params_;
  ast::Expr *fn_ret_type_;

  ast::BlockStmt *fn_body_;
  std::list<ast::BlockStmt *> blocks_;
  std::list<ast::Stmt *> final_stmts_;
};

}  // namespace terrier::execution::compiler
