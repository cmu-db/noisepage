#include "execution/codegen/expression/unary_translator.h"

#include "common/exception.h"
#include "execution/codegen/compilation_context.h"
#include "execution/codegen/work_context.h"

namespace terrier::execution::codegen {

UnaryTranslator::UnaryTranslator(const parser::OperatorExpression &expr, CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *UnaryTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  auto input = ctx->DeriveValue(*GetExpression().GetChild(0), provider);

  parsing::Token::Type type;
  switch (GetExpression().GetExpressionType()) {
    case parser::ExpressionType::OPERATOR_UNARY_MINUS:
      type = parsing::Token::Type::MINUS;
      break;
    case parser::ExpressionType::OPERATOR_NOT:
      type = parsing::Token::Type::BANG;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen->UnaryOp(type, input);
}

}  // namespace terrier::execution::codegen
