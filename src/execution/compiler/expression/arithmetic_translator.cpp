#include "execution/compiler/expression/arithmetic_translator.h"

#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ArithmeticTranslator::ArithmeticTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).Get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).Get(), codegen_)) {}

ast::Expr *ArithmeticTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto *left_expr = left_->DeriveExpr(evaluator);
  auto *right_expr = right_->DeriveExpr(evaluator);
  parsing::Token::Type op_token;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_DIVIDE:
      op_token = parsing::Token::Type::SLASH;
      break;
    case terrier::parser::ExpressionType::OPERATOR_PLUS:
      op_token = parsing::Token::Type::PLUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MINUS:
      op_token = parsing::Token::Type::MINUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MULTIPLY:
      op_token = parsing::Token::Type::STAR;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MOD:
      op_token = parsing::Token::Type::PERCENT;
      break;
    default:
      // TODO(tanujnay112): figure out concatenation operation from expressions?
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->BinaryOp(op_token, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
