#include "execution/compiler/expression/conjunction_translator.h"

#include "common/managed_pointer.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"
#include "execution/parsing/token.h"
#include "execution/util/execution_common.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::execution::ast {
class Expr;
}  // namespace terrier::execution::ast

namespace terrier::execution::compiler {

ConjunctionTranslator::ConjunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).Get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).Get(), codegen_)) {}

ast::Expr *ConjunctionTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto *left_expr = left_->DeriveExpr(evaluator);
  auto *right_expr = right_->DeriveExpr(evaluator);
  parsing::Token::Type op_token;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::CONJUNCTION_OR:
      op_token = parsing::Token::Type::OR;
      break;
    case terrier::parser::ExpressionType::CONJUNCTION_AND:
      op_token = parsing::Token::Type::AND;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->BinaryOp(op_token, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
