#pragma once

#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/codegen.h"

namespace noisepage::execution::compiler {

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
