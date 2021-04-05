#include "execution/compiler/expression/null_check_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/operator_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

NullCheckTranslator::NullCheckTranslator(const parser::OperatorExpression &expr,
                                         CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *NullCheckTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto input = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  switch (auto type = GetExpression().GetExpressionType()) {
    case parser::ExpressionType::OPERATOR_IS_NULL:
      return codegen->CallBuiltin(ast::Builtin::IsValNull, {input});
    case parser::ExpressionType::OPERATOR_IS_NOT_NULL:
      return codegen->UnaryOp(parsing::Token::Type::BANG, codegen->CallBuiltin(ast::Builtin::IsValNull, {input}));
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("operator expression type {}", parser::ExpressionTypeToString(type)));
  }
}

}  // namespace noisepage::execution::compiler
