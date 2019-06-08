#pragma once

#include "execution/util/macros.h"

namespace tpl::compiler {

class CompilationContext;
class Pipeline;
class RowBatch;

/**
 * ConsumerContext: pushes rows over a pipeline.
 */
class ConsumerContext {
 public:
  /**
   * Constructor
   * @param compilation_context the compilation context to use
   * @param pipeline the current pipeline
   */
  ConsumerContext(CompilationContext *compilation_context, Pipeline *pipeline);

  /// Prevent copy and move
  DISALLOW_COPY_AND_MOVE(ConsumerContext);

  /**
   * Pushes a row through the pipeline
   * @param batch row to push
   */
  void Consume(RowBatch *batch);

  /**
   * @return the compilation context
   */
  CompilationContext *GetCompilationContext() { return compilation_context_; }

  /**
   * @return the current pipeline
   */
  const Pipeline *GetPipeline() const { return pipeline_; }

 private:
  CompilationContext *compilation_context_;
  Pipeline *pipeline_;
};
}  // namespace tpl::compiler
