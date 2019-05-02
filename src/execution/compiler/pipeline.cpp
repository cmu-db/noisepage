#include "execution/compiler/pipeline.h"

#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

Pipeline::Pipeline(tpl::compiler::CompilationContext *ctx)
: ctx_(ctx), id_(ctx_->RegisterPipeline(this)), pipeline_index_(0), parallelism_(Pipeline::Parallelism::FLEXIBLE) {}

void Pipeline::Add(OperatorTranslator *translator, Parallelism parallelism) {
  pipeline_index_ = pipeline_.size();
  pipeline_.emplace_back(translator);
  parallelism_ = std::min(parallelism_, parallelism);
}



}