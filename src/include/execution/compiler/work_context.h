#pragma once

#include <functional>
#include <unordered_map>
#include <utility>

#include "execution/compiler/ast_fwd.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/pipeline.h"

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

namespace terrier::execution::compiler {

class CompilationContext;
class FunctionBuilder;

/**
 * A work context carries information necessary for a pipeline along all operators within that
 * pipeline. It provides access to thread-local state and a mechanism to evaluation expressions in
 * the pipeline.
 */
class WorkContext {
 public:
  /**
   * Key for expression translator cache
   */
  using CacheKey_t = std::pair<const parser::AbstractExpression *, const ColumnValueProvider *>;

  /**
   * Create a new context whose data flows along the provided pipeline.
   * @param compilation_context The compilation context.
   * @param pipeline The pipeline.
   */
  WorkContext(CompilationContext *compilation_context, const Pipeline &pipeline);

  /**
   * Derive the value of the given expression.
   * @param expr The expression.
   * @param provider The provider from which column values can be obtained.
   * @return The TPL value of the expression.
   */
  ast::Expr *DeriveValue(const parser::AbstractExpression &expr, const ColumnValueProvider *provider);

  /**
   * Push this context through to the next step in the pipeline.
   * @param function The function that's being built.
   */
  void Push(FunctionBuilder *function);

  /**
   * Clear any cached expression result values.
   */
  void ClearExpressionCache();

  /**
   * @return The operator the context is currently positioned at in the pipeline.
   */
  OperatorTranslator *CurrentOp() const { return *pipeline_iter_; }

  /**
   * Sets the context's position in the pipeline to the given operator
   * @param op The operator to which the context's position will be set
   */
  void SetSource(OperatorTranslator *op);

  /**
   * @return The pipeline the consumption occurs in.
   */
  const Pipeline &GetPipeline() const { return pipeline_; }

  /**
   * @return True if the pipeline this work is flowing on is parallel; false otherwise.
   */
  bool IsParallel() const;

  /**
   * Controls whether expression caching is enabled in this context.
   * @param val True if caching is enabled; false otherwise.
   */
  void SetExpressionCacheEnable(bool val) { cache_enabled_ = val; }

 private:
  // The compilation context.
  CompilationContext *compilation_context_;
  // The pipeline that this context flows through.
  const Pipeline &pipeline_;

  struct HashKey {
    size_t operator()(const CacheKey_t &p) const {
      auto hash1 = std::hash<CacheKey_t::first_type>{}(p.first);
      auto hash2 = std::hash<CacheKey_t::second_type>{}(p.second);
      return hash1 ^ hash2;
    }
  };

  // Cache of expression results.
  std::unordered_map<CacheKey_t, ast::Expr *, HashKey> cache_;
  // The current pipeline step and last pipeline step.
  Pipeline::StepIterator pipeline_iter_, pipeline_end_;
  // Whether to cache translated expressions
  bool cache_enabled_;
};

}  // namespace terrier::execution::compiler
