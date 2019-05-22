#include "execution/compiler/codegen.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/pipeline.h"

namespace tpl::compiler {

const OperatorTranslator *Pipeline::NextStep() {
  if (pipeline_index_ > 0) {
    return pipeline_[--pipeline_index_];
  }
  return nullptr;
}

util::Region *Pipeline::GetRegion() { return ctx_->GetRegion(); }

CodeGen *Pipeline::GetCodeGen() { return ctx_->GetCodeGen(); }

void Pipeline::Add(OperatorTranslator *translator, Pipeline::Parallelism parallelism) {
  pipeline_.push_back(translator);
}
}  // namespace tpl::compiler
