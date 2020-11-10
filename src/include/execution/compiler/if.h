#pragma once

#include "execution/compiler/ast_fwd.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::compiler {

class FunctionBuilder;

/**
 * Helper class to generate TPL if-then-else statements. Immediately after construction, anything
 * that's appended to the current active function will be inserted into the "then" clause of the
 * generated statement.
 *
 * @code
 * auto cond = codegen->CompareLt(a, b);
 * If a_lt_b(codegen, cond);
 * {
 *   // This code will appear in the "then" block of the statement.
 * }
 * a_lt_b.Else();
 * {
 *   // This code will appear in the "else" block of the statement.
 * }
 * a_lt_b.EndIf();
 * @endcode
 */
class If {
 public:
  /**
   * Create a new if-statement using the provided boolean if-condition.
   * @param function The function this if statement is appended to.
   * @param condition The boolean condition.
   */
  If(FunctionBuilder *function, ast::Expr *condition);

  /**
   * Destructor will complete the statement.
   */
  ~If();

  /**
   * Begin an else clause.
   */
  void Else();

  /**
   * Finish the if-then-else statement. The generated statement will be appended to the current
   * function.
   */
  void EndIf();

 private:
  // The function to append this if statement to.
  FunctionBuilder *function_;
  // The start position;
  const SourcePosition position_;
  // Previous function statement list.
  ast::BlockStmt *prev_func_stmt_list_;
  // The condition.
  ast::Expr *condition_;
  // The statements in the "then" clause.
  ast::BlockStmt *then_stmts_;
  // The statements in the "else" clause.
  ast::BlockStmt *else_stmts_;
  // Flag indicating if the if-statement has completed.
  bool completed_;
};

}  // namespace noisepage::execution::compiler
