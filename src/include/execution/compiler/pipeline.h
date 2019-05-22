#pragma once

#include <vector>
#include "execution/util/region.h"

namespace tpl::compiler {

class CompilationContext;
class OperatorTranslator;
class CodeGen;

class Pipeline {
 public:
  explicit Pipeline(CompilationContext *ctx) : ctx_(ctx), pipeline_index_(0) {}

  enum class Parallelism : uint32_t { Serial = 0, Flexible = 1, Parallel = 2 }

  util::Region *GetRegion();

  CodeGen *GetCodeGen();

  CompilationContext *GetCompilationContext() { return ctx_; }

  void Add(OperatorTranslator *translator, Parallelism parallelism);

  const OperatorTranslator *NextStep();

 private:
  CompilationContext *ctx_;
  std::vector<OperatorTranslator *> pipeline_;
  uint32_t pipeline_index_;
};

}  // namespace tpl::compiler
