#include "execution/compiler/expression/constant_translator.h"

#include "execution/compiler/codegen.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier {
namespace execution::ast {
class Expr;
}  // namespace execution::ast
namespace parser {
class AbstractExpression;
}  // namespace parser
}  // namespace terrier

namespace terrier::execution::compiler {
ConstantTranslator::ConstantTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ConstantTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto const_val = GetExpressionAs<terrier::parser::ConstantValueExpression>();
  auto trans_val = const_val->GetValue();
  return codegen_->PeekValue(trans_val);
}
};  // namespace terrier::execution::compiler
