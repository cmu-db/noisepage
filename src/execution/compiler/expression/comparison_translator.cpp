#include "execution/compiler/expression/comparison_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/comparison_expression.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::compiler {

ComparisonTranslator::ComparisonTranslator(const parser::ComparisonExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ComparisonTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::COMPARE_EQUAL:
    case parser::ExpressionType::COMPARE_IN:
      return codegen->Compare(parsing::Token::Type::EQUAL_EQUAL, left_val, right_val);
    case parser::ExpressionType::COMPARE_GREATER_THAN:
      return codegen->Compare(parsing::Token::Type::GREATER, left_val, right_val);
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      return codegen->Compare(parsing::Token::Type::GREATER_EQUAL, left_val, right_val);
    case parser::ExpressionType::COMPARE_LESS_THAN:
      return codegen->Compare(parsing::Token::Type::LESS, left_val, right_val);
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      return codegen->Compare(parsing::Token::Type::LESS_EQUAL, left_val, right_val);
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
      return codegen->Compare(parsing::Token::Type::BANG_EQUAL, left_val, right_val);
    case parser::ExpressionType::COMPARE_LIKE:
      return codegen->Like(left_val, right_val);
    case parser::ExpressionType::COMPARE_NOT_LIKE:
      return codegen->NotLike(left_val, right_val);
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("Translation of comparison type {}", parser::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace terrier::execution::compiler
