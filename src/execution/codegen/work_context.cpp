#include "execution/codegen/work_context.h"

#include "execution/codegen/compilation_context.h"
#include "execution/codegen/pipeline.h"

namespace terrier::execution::codegen {

WorkContext::WorkContext(CompilationContext *compilation_context, const Pipeline &pipeline)
    : compilation_context_(compilation_context),
      pipeline_(pipeline),
      pipeline_iter_(pipeline_.Begin()),
      pipeline_end_(pipeline_.End()),
      cache_enabled_(true) {}

ast::Expr *WorkContext::DeriveValue(const parser::AbstractExpression &expr, const ColumnValueProvider *provider) {
  if (cache_enabled_) {
    if (auto iter = cache_.find(&expr); iter != cache_.end()) {
      return iter->second;
    }
  }
  auto *translator = compilation_context_->LookupTranslator(expr);
  if (translator == nullptr) {
    return nullptr;
  }
  auto result = translator->DeriveValue(this, provider);
  if (cache_enabled_) cache_[&expr] = result;
  return result;
}

void WorkContext::Push(FunctionBuilder *function) {
  if (++pipeline_iter_ == pipeline_end_) {
    return;
  }
  (*pipeline_iter_)->PerformPipelineWork(this, function);
}

void WorkContext::ClearExpressionCache() { cache_.clear(); }

bool WorkContext::IsParallel() const { return pipeline_.IsParallel(); }

}  // namespace terrier::execution::codegen
