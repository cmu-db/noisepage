#include "execution/compiler/expression/comparison_translator.h"

#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ComparisonTranslator::ComparisonTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).Get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).Get(), codegen_)) {}

ast::Expr *ComparisonTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto *left_expr = left_->DeriveExpr(evaluator);
  auto *right_expr = right_->DeriveExpr(evaluator);
  parsing::Token::Type op_token;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::COMPARE_EQUAL:
      op_token = parsing::Token::Type::EQUAL_EQUAL;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN:
      op_token = parsing::Token::Type::GREATER;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      op_token = parsing::Token::Type::GREATER_EQUAL;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN:
      op_token = parsing::Token::Type::LESS;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      op_token = parsing::Token::Type::LESS_EQUAL;
      break;
    case terrier::parser::ExpressionType::COMPARE_NOT_EQUAL:
      op_token = parsing::Token::Type::BANG_EQUAL;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->Compare(op_token, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
