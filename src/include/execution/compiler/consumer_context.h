#pragma once

#include "execution/util/macros.h"

namespace tpl::compiler {

class CompilationContext;
class Pipeline;
class RowBatch;

class ConsumerContext {
 public:
  ConsumerContext(CompilationContext *compilation_context, Pipeline *pipeline);
  DISALLOW_COPY_AND_MOVE(ConsumerContext);

  void Consume(RowBatch *batch);

  CompilationContext *GetCompilationContext() { return compilation_context_; }
  const Pipeline *GetPipeline() const { return pipeline_; }

 private:
  CompilationContext *compilation_context_;
  Pipeline *pipeline_;
};
}  // namespace tpl::compiler
