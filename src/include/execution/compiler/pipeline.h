#pragma once

#include <vector>
#include "execution/util/region.h"

namespace tpl::compiler {

class CompilationContext;
class OperatorTranslator;
class CodeGen;

/// A single pipeline
class Pipeline {
 public:
  /**
   * Constructor
   * @param ctx compilation context to use
   */
  explicit Pipeline(CompilationContext *ctx) : ctx_(ctx), pipeline_index_(0) {}

  /// Parallism level to use
  enum class Parallelism : uint32_t { Serial = 0, Flexible = 1, Parallel = 2 };

  /**
   * @return region used for allocation
   */
  util::Region *GetRegion();

  /**
   * @return code generator
   */
  CodeGen *GetCodeGen();

  /**
   * @return compilation context
   */
  CompilationContext *GetCompilationContext() { return ctx_; }

  /**
   * Add an operator translator to the pipeline
   * @param translator translator to add
   * @param parallelism parallelism level
   */
  void Add(OperatorTranslator *translator, Parallelism parallelism);

  /**
   * @return next operator translator
   */
  const OperatorTranslator *NextStep();

 private:
  CompilationContext *ctx_;
  std::vector<OperatorTranslator *> pipeline_;
  uint32_t pipeline_index_;
};

}  // namespace tpl::compiler
