#pragma once

#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

// TODO(WAN): amadou, your output buffer stuff should hook up here
class ExecutionConsumer {
 public:
  virtual void Prepare(CompilationContext *ctx);               // before codegen
  virtual void InitializeQueryState(CompilationContext *ctx);  // init block
  virtual void TeardownQueryState(CompilationContext *ctx);    // teardown block

 private:
};

}  // namespace tpl::compiler