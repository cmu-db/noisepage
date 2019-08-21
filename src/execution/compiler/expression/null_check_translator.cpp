#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
NullCheckTranslator::NullCheckTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      child_{TranslatorFactory::CreateExpressionTranslator(expression->GetChild(0).get(), codegen)} {}

ast::Expr *NullCheckTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto type = expression_->GetExpressionType();
  auto child_expr = child_->DeriveExpr(translator);
  auto null_expr = codegen_->NilLiteral();
  if (type == terrier::parser::ExpressionType::OPERATOR_IS_NULL) {
    return codegen_->BinaryOp(parsing::Token::Type::EQUAL_EQUAL, null_expr, child_expr);
  }
  TERRIER_ASSERT(type == terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL, "Unsupported expression");
  return codegen_->BinaryOp(parsing::Token::Type::BANG_EQUAL, null_expr, child_expr);
}
};  // namespace terrier::execution::compiler
