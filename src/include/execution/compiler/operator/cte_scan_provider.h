#pragma once

namespace noisepage::execution::ast {
class Expr;
}  // namespace noisepage::execution::ast

namespace noisepage::execution::compiler {
class CodeGen;

/**
 * An interface for classes that expose a readable cte scan iterator
 */
class CteScanProvider {
 public:
  /**
   * @param codegen The current code generator object
   * @return A pointer to a readable cte scan iterator
   */
  virtual ast::Expr *GetCteScanPtr(CodeGen *codegen) const = 0;
};
}  // namespace noisepage::execution::compiler
