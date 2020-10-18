#include "execution/compiler/expression/constant_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/work_context.h"
#include "execution/sql/generic_value.h"
#include "parser/expression/constant_value_expression.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::compiler {

ConstantTranslator::ConstantTranslator(const parser::ConstantValueExpression &expr,
                                       CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {}

ast::Expr *ConstantTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  const auto &val = GetExpressionAs<const parser::ConstantValueExpression>();
  const auto type_id = sql::GetTypeId(val.GetReturnValueType());

  if (val.IsNull()) {
    return codegen->ConstNull(val.GetReturnValueType());
  }

  switch (type_id) {
    case sql::TypeId::Boolean:
      return codegen->BoolToSql(val.GetBoolVal().val_);
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
      return codegen->IntToSql(val.GetInteger().val_);
    case sql::TypeId::Float:
    case sql::TypeId::Double:
      return codegen->FloatToSql(val.GetReal().val_);
    case sql::TypeId::Date:
      return codegen->DateToSql(val.GetDateVal().val_);
    case sql::TypeId::Timestamp:
      return codegen->TimestampToSql(val.GetTimestampVal().val_);
    case sql::TypeId::Varchar:
      return codegen->StringToSql(val.GetStringVal().StringView());
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("Translation of constant type {}", TypeIdToString(type_id)));
  }
}

}  // namespace terrier::execution::compiler
