#pragma once

namespace tpl::compiler {

class CompilationContext;

class ExecutionConsumer {
 public:
  void Prepare(CompilationContext *ctx);
  void InitializeQueryState(CompilationContext *ctx);
};

}