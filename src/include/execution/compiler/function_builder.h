#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

class CodeGen;

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
  FunctionBuilder(CodeGen *codegen, ast::Identifier fn_name, util::RegionVector<ast::FieldDecl *> fn_params,
                  ast::Expr *fn_ret_type);

  /// Prevent copy and move
  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  /**
   * Appends a stmt to the current block
   * @param stmt to append.
   */
  void Append(ast::Stmt *stmt) { insertion_point_->AppendStmt(stmt); }

  /**
   * Sets the current block
   * @param insertion_point new block
   */
  void SetInsertionPoint(ast::BlockStmt *insertion_point) { insertion_point_ = insertion_point; }

  /**
   * Begins a ForInStmt
   * @param target target variable
   * @param table table to iterate over
   * @param attributes iteration attributes.
   */
  void StartForInStmt(ast::Expr *target, ast::Expr *table, ast::Attributes *attributes);

  /**
   * Begins an IfStmt
   * @param condition if condition
   */
  void StartIfStmt(ast::Expr *condition);

  /**
   * @return the code generator used
   */
  CodeGen *GetCodeGen() { return codegen_; }

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
  ast::BlockStmt *insertion_point_;
};

}  // namespace tpl::compiler
