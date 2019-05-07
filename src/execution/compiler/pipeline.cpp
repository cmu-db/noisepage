#include "execution/compiler/codegen.h"
#include "execution/compiler/pipeline.h"
#include "../../include/execution/compiler/pipeline.h"

namespace tpl::compiler {

 const OperatorTranslator *Pipeline::NextStep() {
   if (pipeline_index_ > 0) {
     return pipeline_[--pipeline_index_];
   } else {
     return nullptr;
   }
 }

  util::Region *Pipeline::GetRegion() {
    return ctx_->GetRegion();
  };

  CodeGen &Pipeline::GetCodeGen() {
    return ctx_->GetCodeGen();
  }

  void compiler::Pipeline::Add(OperatorTranslator *translator, tpl::compiler::Pipeline::Parallelism
    parallelism) {
    pipeline_.push_back(translator);
  }
}