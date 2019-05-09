#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {
NullCheckTranslator::NullCheckTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext &context)
    : ExpressionTranslator(expression, context) {
  context.Prepare(*expression->GetChild(0));
};

ast::Expr *NullCheckTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) {
  auto type = expression->GetExpressionType();
  auto codegen = context_.GetCodeGen();
  auto child = row.DeriveValue(*expression->GetChild(0));
  auto null_expr = (*codegen)->NewNilLiteral(DUMMY_POS);
  if(type == terrier::parser::ExpressionType::OPERATOR_IS_NULL){
    return (*codegen)->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::EQUAL_EQUAL, null_expr, child);
  }
  TPL_ASSERT(type == terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL, "Unsupported expression");
  return (*codegen)->NewBinaryOpExpr(DUMMY_POS, parsing::Token::Type::BANG_EQUAL, null_expr, child);
}
};