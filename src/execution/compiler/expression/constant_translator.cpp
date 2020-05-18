#include "execution/compiler/expression/constant_translator.h"

#include "execution/compiler/translator_factory.h"
#include "execution/sql/value.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::compiler {
ConstantTranslator::ConstantTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ConstantTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  const auto *const const_val = GetExpressionAs<terrier::parser::ConstantValueExpression>();
  return codegen_->PeekValue(*const_val);
}
};  // namespace terrier::execution::compiler
