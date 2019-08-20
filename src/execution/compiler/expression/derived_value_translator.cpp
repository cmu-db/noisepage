#include "execution/compiler/expression/derived_value_translator.h"
#include "parser/expression/derived_value_expression.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/operator/seq_scan_translator.h"


namespace terrier::execution::compiler {
DerivedValueTranslator::DerivedValueTranslator(const terrier::parser::AbstractExpression *expression,
                                             CodeGen * codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *DerivedValueTranslator::DeriveExpr(OperatorTranslator * translator) {
  auto derived_val = GetExpressionAs<terrier::parser::DerivedValueExpression>();
  return translator->GetChildOutput(derived_val->GetTupleIdx(), derived_val->GetValueIdx(), derived_val->GetReturnValueType());
}
};  // namespace terrier::execution::compiler
