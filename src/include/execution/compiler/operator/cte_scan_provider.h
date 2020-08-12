#pragma once

#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/codegen.h"

namespace terrier::execution::compiler {

class CteScanProvider {
 public:
  virtual ast::Expr *GetCteScanPtr(CodeGen *codegen) const = 0;
};
}  // namespace terrier::execution::compiler
