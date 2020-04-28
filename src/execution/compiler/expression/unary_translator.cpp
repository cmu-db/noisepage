#include "execution/compiler/expression/unary_translator.h"

#include "common/managed_pointer.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"
#include "execution/parsing/token.h"
#include "execution/util/execution_common.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier {
namespace execution {
namespace ast {
class Expr;
}  // namespace ast
}  // namespace execution
}  // namespace terrier

namespace terrier::execution::compiler {

UnaryTranslator::UnaryTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      child_(TranslatorFactory::CreateExpressionTranslator(expression->GetChild(0).Get(), codegen)) {}

ast::Expr *UnaryTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto *child_expr = child_->DeriveExpr(evaluator);
  parsing::Token::Type op_token;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_UNARY_MINUS:
      op_token = parsing::Token::Type::MINUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_NOT:
      op_token = parsing::Token::Type::BANG;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->UnaryOp(op_token, child_expr);
}
}  // namespace terrier::execution::compiler
