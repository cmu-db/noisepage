#include "execution/compiler/expression/param_value_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/value.h"
#include "parser/expression/parameter_value_expression.h"
#include "type/transient_value_peeker.h"

namespace terrier::execution::compiler {
ParamValueTranslator::ParamValueTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ParamValueTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto param_val = GetExpressionAs<terrier::parser::ParameterValueExpression>();
  auto param_idx = param_val->GetValueIdx();
  ast::Builtin builtin;
  switch (param_val->GetReturnValueType()) {
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
    case type::TypeId::DECIMAL:
      builtin = ast::Builtin::GetParamDouble;
      break;
    case type::TypeId::DATE:
      builtin = ast::Builtin::GetParamDate;
      break;
    case type::TypeId::VARCHAR:
      builtin = ast::Builtin::GetParamString;
      break;
    default:
      UNREACHABLE("Unsupported parameter type");
  }

  return codegen_->BuiltinCall(builtin,
                               {codegen_->MakeExpr(codegen_->GetExecCtxVar()), codegen_->IntLiteral(param_idx)});
}
};  // namespace terrier::execution::compiler
