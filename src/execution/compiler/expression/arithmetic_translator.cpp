#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ArithmeticTranslator::ArithmeticTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).get(), codegen_)) {}

ast::Expr *ArithmeticTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto *left_expr = left_->DeriveExpr(translator);
  auto *right_expr = right_->DeriveExpr(translator);
  parsing::Token::Type type;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_DIVIDE:
      type = parsing::Token::Type::SLASH;
      break;
    case terrier::parser::ExpressionType::OPERATOR_PLUS:
      type = parsing::Token::Type::PLUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MULTIPLY:
      type = parsing::Token::Type::STAR;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MOD:
      type = parsing::Token::Type::PERCENT;
      break;
    default:
      // TODO(tanujnay112): figure out concatenation operation from expressions?
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->BinaryOp(type, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
