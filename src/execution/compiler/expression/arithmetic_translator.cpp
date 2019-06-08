#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

ArithmeticTranslator::ArithmeticTranslator(const terrier::parser::AbstractExpression *expression,
                                           CompilationContext *context)
    : ExpressionTranslator(expression, context) {
  context->Prepare(*expression->GetChild(0));
  context->Prepare(*expression->GetChild(1));
}

ast::Expr *ArithmeticTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) {
  auto arith_expr = GetExpressionAs<terrier::parser::OperatorExpression>();
  auto *left = row->DeriveValue(*expression->GetChild(0));
  auto *right = row->DeriveValue(*expression->GetChild(1));
  parsing::Token::Type type;
  switch (expression->GetExpressionType()) {
    case terrier::parser::ExpressionType::OPERATOR_DIVIDE:
      type = parsing::Token::Type::SLASH;
      break;
    case terrier::parser::ExpressionType::OPERATOR_PLUS:
      type = parsing::Token::Type::PLUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MULTIPLY:
      type = parsing::Token::Type::STAR;
      break;
    case terrier::parser::ExpressionType::OPERATOR_MOD:
      type = parsing::Token::Type::PERCENT;
      break;
    default:
      // TODO(tanujnay112): figure out concatenation operation from expressions?
      TPL_ASSERT(false, "Unsupported expression");
  }
  return (*context_->GetCodeGen())->NewBinaryOpExpr(DUMMY_POS, type, left, right);
}
}  // namespace tpl::compiler
