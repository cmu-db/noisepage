#include "execution/codegen/expression/conjunction_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "execution/codegen/codegen.h"
#include "execution/codegen/compilation_context.h"
#include "execution/codegen/work_context.h"

namespace terrier::execution::codegen {

ConjunctionTranslator::ConjunctionTranslator(const parser::ConjunctionExpression &expr,
                                             CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ConjunctionTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::CONJUNCTION_AND:
      return codegen->BinaryOp(parsing::Token::Type::AND, left_val, right_val);
    case parser::ExpressionType::CONJUNCTION_OR:
      return codegen->BinaryOp(parsing::Token::Type::OR, left_val, right_val);
    default: {
      throw NotImplementedException(
          fmt::format("Translation of conjunction type {}", parser::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace terrier::execution::codegen
