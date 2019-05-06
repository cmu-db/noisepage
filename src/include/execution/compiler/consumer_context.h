#pragma once
#include "execution/compiler/pipeline.h"
#include "execution/compiler/row_batch.h"

namespace tpl::compiler {
class ConsumerContext {
 public:
  // Constructor
  ConsumerContext(CompilationContext &compilation_context, Pipeline *pipeline);

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(ConsumerContext);

  // Pass this consumer context to the parent of the caller of consume()
  void Consume(RowBatch &batch);

  CompilationContext &GetCompilationContext() { return compilation_context_; }

  // Get the code generator instance
  CodeGen &GetCodeGen() const;

  // Get the pipeline and the context
  const Pipeline &GetPipeline() const { return pipeline_; }

 private:
  // The compilation context
  CompilationContext &compilation_context_;

  // The pipeline of operators that this context passes through
  Pipeline *pipeline_;
};
}