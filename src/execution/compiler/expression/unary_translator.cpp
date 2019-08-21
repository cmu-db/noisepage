#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

UnaryTranslator::UnaryTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      child_(TranslatorFactory::CreateExpressionTranslator(expression->GetChild(0).get(), codegen)) {}

ast::Expr *UnaryTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto *child_expr = child_->DeriveExpr(translator);
  parsing::Token::Type type;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_UNARY_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_NOT:
      type = parsing::Token::Type::BANG;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->UnaryOp(type, child_expr);
}
}  // namespace terrier::execution::compiler
