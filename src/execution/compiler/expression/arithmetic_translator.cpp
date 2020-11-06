#include "execution/compiler/expression/arithmetic_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/operator_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

ArithmeticTranslator::ArithmeticTranslator(const parser::OperatorExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *ArithmeticTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::OPERATOR_PLUS:
      return codegen->BinaryOp(parsing::Token::Type::PLUS, left_val, right_val);
    case parser::ExpressionType::OPERATOR_MINUS:
      return codegen->BinaryOp(parsing::Token::Type::MINUS, left_val, right_val);
    case parser::ExpressionType::OPERATOR_MULTIPLY:
      return codegen->BinaryOp(parsing::Token::Type::STAR, left_val, right_val);
    case parser::ExpressionType::OPERATOR_DIVIDE:
      return codegen->BinaryOp(parsing::Token::Type::SLASH, left_val, right_val);
    case parser::ExpressionType::OPERATOR_MOD:
      return codegen->BinaryOp(parsing::Token::Type::PERCENT, left_val, right_val);
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("Translation of arithmetic type {}", parser::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace noisepage::execution::compiler
