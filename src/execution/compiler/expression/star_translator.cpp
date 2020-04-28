#include "execution/compiler/expression/star_translator.h"

#include "execution/compiler/codegen.h"

namespace terrier {
namespace execution {
namespace ast {
class Expr;
}  // namespace ast
}  // namespace execution
namespace parser {
class AbstractExpression;
}  // namespace parser
}  // namespace terrier

namespace terrier::execution::compiler {
StarTranslator::StarTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *StarTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  // TODO(Amadou): COUNT(*) will increment its counter regardless of the input we pass in.
  // So the value we return here does not matter. The StarExpression can just be replaced by a constant.
  return codegen_->IntToSql(0);
}
};  // namespace terrier::execution::compiler
