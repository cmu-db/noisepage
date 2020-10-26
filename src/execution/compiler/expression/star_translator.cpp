#include "execution/compiler/expression/star_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

StarTranslator::StarTranslator(const parser::AbstractExpression &expr, CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *StarTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  // TODO(Amadou): COUNT(*) will increment its counter regardless of the input we pass in.
  // So the value we return here does not matter. The StarExpression can just be replaced by a constant.
  return codegen->IntToSql(0);
}

}  // namespace noisepage::execution::compiler
