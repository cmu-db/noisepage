#include "execution/compiler/expression/derived_value_translator.h"

#include "parser/expression/derived_value_expression.h"

namespace terrier {
namespace execution {
namespace ast {
class Expr;
}  // namespace ast
namespace compiler {
class CodeGen;
}  // namespace compiler
}  // namespace execution
namespace parser {
class AbstractExpression;
}  // namespace parser
}  // namespace terrier

namespace terrier::execution::compiler {
DerivedValueTranslator::DerivedValueTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *DerivedValueTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto derived_val = GetExpressionAs<terrier::parser::DerivedValueExpression>();
  return evaluator->GetChildOutput(derived_val->GetTupleIdx(), derived_val->GetValueIdx(),
                                   derived_val->GetReturnValueType());
}
};  // namespace terrier::execution::compiler
