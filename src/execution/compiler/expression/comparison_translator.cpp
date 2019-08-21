#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ComparisonTranslator::ComparisonTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).get(), codegen_)) {}

ast::Expr *ComparisonTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto *left_expr = left_->DeriveExpr(translator);
  auto *right_expr = right_->DeriveExpr(translator);
  parsing::Token::Type type;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::COMPARE_EQUAL:
      type = parsing::Token::Type::EQUAL_EQUAL;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN:
      type = parsing::Token::Type::GREATER;
      break;
    case terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      type = parsing::Token::Type::GREATER_EQUAL;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN:
      type = parsing::Token::Type::LESS;
      break;
    case terrier::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      type = parsing::Token::Type::LESS_EQUAL;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->Compare(type, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
