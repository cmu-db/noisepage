#pragma once

#include "common/common.h"
#include "execution/sql/codegen/ast_fwd.h"

namespace terrier::execution::codegen {

class FunctionBuilder;

/**
 * Helper class to generate TPL loops. Immediately after construction, statements appended to the
 * current active function are appended to the loop's body.
 *
 * @code
 * auto cond = codegen->CompareLt(a, b);
 * Loop loop(codegen, cond);
 * {
 *   // This code will appear in the "then" block of the statement.
 * }
 * loop.EndLoop();
 * @endcode
 */
class Loop {
 public:
  /**
   * Create a full loop.
   * @param codegen The code generator.
   * @param init The initialization statements.
   * @param condition The loop condition.
   * @param next The next statements.
   */
  explicit Loop(FunctionBuilder *function, ast::Stmt *init, ast::Expr *condition, ast::Stmt *next);

  /**
   * Create a while-loop.
   * @param codegen The code generator instance.
   * @param condition The loop condition.
   */
  explicit Loop(FunctionBuilder *function, ast::Expr *condition);

  /**
   * Create an infinite loop.
   * @param codegen The code generator instance.
   */
  explicit Loop(FunctionBuilder *function);

  /**
   * Destructor.
   */
  ~Loop();

  /**
   * Explicitly mark the end of a loop.
   */
  void EndLoop();

 private:
  // The function this loop is appended to.
  FunctionBuilder *function_;
  // The loop position.
  const SourcePosition position_;
  // The previous list of statements.
  ast::BlockStmt *prev_statements_;
  // The initial statements, loop condition, and next statements.
  ast::Stmt *init_;
  ast::Expr *condition_;
  ast::Stmt *next_;
  // The loop body.
  ast::BlockStmt *loop_body_;
  // Completion flag.
  bool completed_;
};

}  // namespace terrier::execution::codegen
