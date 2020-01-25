#include "execution/compiler/expression/star_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/value.h"
#include "parser/expression/star_expression.h"
#include "type/transient_value_peeker.h"

namespace terrier::execution::compiler {
StarTranslator::StarTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *StarTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  // TODO(Amadou): What we return here doesn't really matter. Should StarExpression be ConstantValueExpression?
  return codegen_->IntToSql(0);
}
};  // namespace terrier::execution::compiler
