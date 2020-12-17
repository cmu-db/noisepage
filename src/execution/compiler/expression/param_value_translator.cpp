#include "execution/compiler/expression/param_value_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/parameter_value_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

ParamValueTranslator::ParamValueTranslator(const parser::ParameterValueExpression &expr,
                                           CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ParamValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto param_val = GetExpressionAs<noisepage::parser::ParameterValueExpression>();
  auto param_idx = param_val.GetValueIdx();
  ast::Builtin builtin;
  switch (param_val.GetReturnValueType()) {
    case type::TypeId::BOOLEAN:
      builtin = ast::Builtin::GetParamBool;
      break;
    case type::TypeId::TINYINT:
      builtin = ast::Builtin::GetParamTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = ast::Builtin::GetParamSmallInt;
      break;
    case type::TypeId::INTEGER:
      builtin = ast::Builtin::GetParamInt;
      break;
    case type::TypeId::BIGINT:
      builtin = ast::Builtin::GetParamBigInt;
      break;
    case type::TypeId::REAL:
      builtin = ast::Builtin::GetParamDouble;
      break;
    case type::TypeId::DATE:
      builtin = ast::Builtin::GetParamDate;
      break;
    case type::TypeId::TIMESTAMP:
      builtin = ast::Builtin::GetParamTimestamp;
      break;
    case type::TypeId::VARCHAR:
      builtin = ast::Builtin::GetParamString;
      break;
    default:
      UNREACHABLE("Unsupported parameter type");
  }

  return codegen->CallBuiltin(builtin, {GetExecutionContextPtr(), codegen->Const32(param_idx)});
}

}  // namespace noisepage::execution::compiler
