#pragma once

#include <vector>

#include "execution/ast/ast.h"
#include "execution/ast/type.h"
#include "execution/util/common.h"

namespace tpl::compiler {

class CodeGen;
class CompilationContext;
class OperatorTranslator;
class Pipeline;

class PipelineContext {
  friend class Pipeline;

 public:
  using Id = u32;
  explicit PipelineContext(Pipeline *pipeline);
  Id RegisterState(std::string name, ast::Type *type);
  void FinalizeState(CodeGen *codegen);
  ast::Type *GetThreadStateType() const { return thread_state_type_; }

 private:
  Pipeline *pipeline_;
  Id init_flag_id_;
  std::vector<std::pair<std::string, ast::Type *>> state_components_;
  ast::Type *thread_state_type_;
  ast::Expr *thread_state_;
  ast::Expr *thread_init_func_;
  ast::Expr *pipeline_func_;
};

class Pipeline {
 public:
  enum class Parallelism : u8 { SERIAL = 0, FLEXIBLE = 1, PARALLEL = 2 };

  explicit Pipeline(CompilationContext *ctx);

  void Add(OperatorTranslator *translator, Parallelism parallelism);

  void MarkSource(OperatorTranslator *translator, Parallelism parallelism);

 private:
  CompilationContext *ctx_;
  u32 id_;
  u32 pipeline_index_;
  std::vector<OperatorTranslator *> pipeline_;
  std::vector<u32> stage_boundaries_;
  Parallelism parallelism_;
};

}  // namespace tpl::compiler