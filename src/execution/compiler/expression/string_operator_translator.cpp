#include "execution/compiler/expression/string_operator_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/operator_expression.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::compiler {

StringOperatorTranslator::StringOperatorTranslator(const parser::OperatorExpression &expr,
                                                   CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  // Prepare the left and right expression subtrees for translation.
  compilation_context->Prepare(*expr.GetChild(0));
  compilation_context->Prepare(*expr.GetChild(1));
}

ast::Expr *StringOperatorTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto left_val = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto right_val = ctx->DeriveValue(*GetExpression().GetChild(1), provider);

  switch (auto expr_type = GetExpression().GetExpressionType(); expr_type) {
    case parser::ExpressionType::OPERATOR_CONCAT:
      return codegen->StringBinaryOp(parsing::Token::Type::CONCAT, left_val, right_val, GetExecutionContextPtr());
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("Translation of string operator type {}", parser::ExpressionTypeToString(expr_type, true)));
    }
  }
}

}  // namespace terrier::execution::compiler
