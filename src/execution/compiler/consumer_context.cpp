#include "execution/compiler/consumer_context.h"

namespace tpl::compiler {
  ConsumerContext::ConsumerContext(CompilationContext &compilation_context,
                                 Pipeline *pipeline) : compilation_context_(compilation_context), pipeline_(pipeline) {}

// Pass the row batch to the next operator in the pipeline
void ConsumerContext::Consume(RowBatch &batch) {
  auto *translator = pipeline_->NextStep();
  if (translator == nullptr) {
    // We're at the end of the query pipeline, we now send the output tuples
    // to the result consumer configured in the compilation context
    auto consumer = compilation_context_.GetExecutionConsumer();
    consumer->ConsumeResult(*this, batch);
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