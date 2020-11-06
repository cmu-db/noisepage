#include "execution/compiler/expression/conjunction_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/conjunction_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

ConjunctionTranslator::ConjunctionTranslator(const parser::ConjunctionExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare all children expression subtrees for translation.
  for (const auto &child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *ConjunctionTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::CONJUNCTION_AND:
      return codegen->BinaryOp(parsing::Token::Type::AND, left_val, right_val);
    case parser::ExpressionType::CONJUNCTION_OR:
      return codegen->BinaryOp(parsing::Token::Type::OR, left_val, right_val);
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("Translation of conjunction type {}", parser::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace noisepage::execution::compiler
