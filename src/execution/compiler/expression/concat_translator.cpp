#include "execution/compiler/expression/concat_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ConcatTranslator::ConcatTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).Get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).Get(), codegen_)) {}

ast::Expr *ConcatTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto *left_expr = left_->DeriveExpr(evaluator);
  auto *right_expr = right_->DeriveExpr(evaluator);
  parsing::Token::Type op_token;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_CONCAT:
      op_token = parsing::Token::Type::CONCAT;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->Concat(op_token, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
