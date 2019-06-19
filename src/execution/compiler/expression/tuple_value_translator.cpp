#include "execution/compiler/expression/tuple_value_translator.h"
#include "parser/expression/tuple_value_expression.h"
#include "execution/compiler/translator_factory.h"


namespace tpl::compiler {
TupleValueTranslator::TupleValueTranslator(const terrier::parser::AbstractExpression *expression,
                                           CodeGen * codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *TupleValueTranslator::DeriveExpr(OperatorTranslator * translator) {
  auto tuple_val = GetExpressionAs<terrier::parser::ExecTupleValueExpression>();
  return translator->GetChildOutput(tuple_val->GetTupleIdx(), tuple_val->GetColIdx(), tuple_val->GetReturnValueType());
}
};  // namespace tpl::compiler
