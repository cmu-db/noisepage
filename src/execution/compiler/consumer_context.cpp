#include "execution/compiler/consumer_context.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/pipeline.h"

namespace tpl::compiler {

ConsumerContext::ConsumerContext(CompilationContext *compilation_context,
                               Pipeline *pipeline) : compilation_context_(compilation_context), pipeline_(pipeline) {}

void ConsumerContext::Consume(RowBatch *batch) {
  auto *translator = pipeline_->NextStep();
  if (translator == nullptr) {
    // End of query pipeline, send output tuples to compilation context's consumer
    auto consumer = compilation_context_->GetExecutionConsumer();
    consumer->ConsumeResult(this, batch);
  } else {
    // We're not at the end of the pipeline, push the batch through the stages
    do {
      translator->Consume(this, batch);
      // When the call returns here, the pipeline position has been shifted to
      // the start of a new stage.
    } while ((translator = pipeline_->NextStep()) != nullptr);
  }
}

}