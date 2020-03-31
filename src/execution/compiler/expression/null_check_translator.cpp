#include "execution/compiler/expression/null_check_translator.h"

#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
NullCheckTranslator::NullCheckTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      child_{TranslatorFactory::CreateExpressionTranslator(expression->GetChild(0).Get(), codegen)} {}

ast::Expr *NullCheckTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto type = expression_->GetExpressionType();
  auto child_expr = child_->DeriveExpr(evaluator);

  ast::Expr *ret;
  if (type == terrier::parser::ExpressionType::OPERATOR_IS_NULL) {
    ret = codegen_->IsSqlNull(child_expr);
  } else if (type == terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL) {
    ret = codegen_->IsSqlNotNull(child_expr);
  } else {
    UNREACHABLE("Unsupported expression");
  }

  return ret;
}
};  // namespace terrier::execution::compiler
