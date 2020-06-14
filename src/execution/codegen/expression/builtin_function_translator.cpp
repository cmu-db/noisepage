#include "execution/sql/codegen/expression/builtin_function_translator.h"

#include "execution/sql/codegen/codegen.h"
#include "execution/sql/codegen/compilation_context.h"
#include "execution/sql/codegen/work_context.h"

namespace terrier::execution::codegen {

BuiltinFunctionTranslator::BuiltinFunctionTranslator(const planner::BuiltinFunctionExpression &expr,
                                                     CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (const auto &child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *BuiltinFunctionTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  auto func_expr = GetExpressionAs<planner::BuiltinFunctionExpression>();

  // Evaluate the arguments to the function.
  std::vector<ast::Expr *> args;
  args.reserve(func_expr.GetChildrenSize());
  for (const auto &child : func_expr.GetChildren()) {
    args.emplace_back(ctx->DeriveValue(*child, provider));
  }

  // Issue the function.
  return codegen->CallBuiltin(func_expr.GetBuiltin(), args);
}

}  // namespace terrier::execution::codegen
