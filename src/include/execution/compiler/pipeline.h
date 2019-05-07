#pragma once
#include "execution/util/region.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {
class CompilationContext;
class CodeGen;

class Pipeline {
 public:
  explicit Pipeline(CompilationContext *ctx) : ctx_(ctx) {};

  enum class Parallelism : uint32_t { Serial = 0, Flexible = 1, Parallel = 2 };

  util::Region *GetRegion() {
    return ctx_->GetRegion();
  };

  CodeGen &GetCodeGen() {
    return ctx_->GetCodeGen();
  }

  CompilationContext *GetCompilationContext() {
    return ctx_;
  }

  void Add(OperatorTranslator *translator, Parallelism parallelism);

  const OperatorTranslator *NextStep();


 private:
  CompilationContext *ctx_;

  std::vector<OperatorTranslator *> pipeline_;
};

}