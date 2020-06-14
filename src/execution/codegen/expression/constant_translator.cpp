#include "execution/sql/codegen/expression/constant_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "execution/sql/codegen/codegen.h"
#include "execution/sql/codegen/work_context.h"
#include "execution/sql/generic_value.h"

namespace terrier::execution::codegen {

ConstantTranslator::ConstantTranslator(const planner::ConstantValueExpression &expr,
                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ConstantTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto codegen = GetCodeGen();
  const auto &val = GetExpressionAs<const planner::ConstantValueExpression>().GetValue();
  switch (val.GetTypeId()) {
    case TypeId::Boolean:
      return codegen->BoolToSql(val.value_.boolean);
    case TypeId::TinyInt:
      return codegen->IntToSql(val.value_.tinyint);
    case TypeId::SmallInt:
      return codegen->IntToSql(val.value_.smallint);
    case TypeId::Integer:
      return codegen->IntToSql(val.value_.integer);
    case TypeId::BigInt:
      return codegen->IntToSql(val.value_.bigint);
    case TypeId::Float:
      return codegen->FloatToSql(val.value_.float_);
    case TypeId::Double:
      return codegen->FloatToSql(val.value_.double_);
    case TypeId::Date:
      return codegen->DateToSql(val.value_.date_);
    case TypeId::Varchar:
      return codegen->StringToSql(val.str_value_);
    default:
      throw NotImplementedException(fmt::format("Translation of constant type {}", TypeIdToString(val.GetTypeId())));
  }
}

}  // namespace terrier::execution::codegen
