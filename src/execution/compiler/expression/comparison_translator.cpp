#include "execution/compiler/expression/comparison_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/comparison_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

ComparisonTranslator::ComparisonTranslator(const parser::ComparisonExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare all expression subtrees for translation.
  NOISEPAGE_ASSERT(expr.GetChildrenSize() == 2 || expr.GetExpressionType() == parser::ExpressionType::COMPARE_IN,
                   "Every ComparisonExpression should have exactly two children, except for COMPARE_IN.");
  NOISEPAGE_ASSERT((expr.GetExpressionType() != parser::ExpressionType::COMPARE_IN) ||
                       (expr.GetChildrenSize() >= 2 && expr.GetExpressionType() == parser::ExpressionType::COMPARE_IN),
                   "Every COMPARE_IN ComparisonExpression should have at least two children.");
  for (const auto &child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *ComparisonTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (const auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::COMPARE_IN: {
      // Given "foo IN (1, 2, 3, ..., N)", produce "(foo == 1) OR (foo == 2) OR ... OR (foo == N)".
      // Convention: child 0 is "foo", child 1 is the first listed value, and these are handled separately at the start.
      auto *final_expr = codegen->Compare(parsing::Token::Type::EQUAL_EQUAL, left_val, right_val);
      for (size_t i = 2; i < GetExpression().GetChildrenSize(); ++i) {
        right_val = ctx->DeriveValue(*GetExpression().GetChild(i), provider);
        auto *curr_expr = codegen->Compare(parsing::Token::Type::EQUAL_EQUAL, left_val, right_val);
        final_expr = codegen->BinaryOp(parsing::Token::Type::OR, final_expr, curr_expr);
      }
      return final_expr;
    }
    case parser::ExpressionType::COMPARE_EQUAL:
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

}  // namespace noisepage::execution::compiler
