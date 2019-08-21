#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/value.h"
#include "parser/expression/constant_value_expression.h"
#include "type/transient_value_peeker.h"

namespace terrier::execution::compiler {
ConstantTranslator::ConstantTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ConstantTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto const_val = GetExpressionAs<terrier::parser::ConstantValueExpression>();
  auto trans_val = const_val->GetValue();
  auto type = trans_val.Type();
  switch (type) {
    case terrier::type::TypeId::TINYINT:
      return codegen_->IntToSql(terrier::type::TransientValuePeeker::PeekTinyInt(trans_val));
    case terrier::type::TypeId::SMALLINT:
      return codegen_->IntToSql(terrier::type::TransientValuePeeker::PeekSmallInt(trans_val));
    case terrier::type::TypeId::INTEGER:
      return codegen_->IntToSql(terrier::type::TransientValuePeeker::PeekInteger(trans_val));
    case terrier::type::TypeId::BIGINT:
      return codegen_->IntToSql(static_cast<int32_t>(terrier::type::TransientValuePeeker::PeekBigInt(trans_val)));
    case terrier::type::TypeId::BOOLEAN:
      // TODO(Amadou): Convert this to sql types if necessary
      return codegen_->BoolLiteral(terrier::type::TransientValuePeeker::PeekBoolean(trans_val));
    case terrier::type::TypeId::DATE: {
      sql::Date date(terrier::type::TransientValuePeeker::PeekDate(trans_val));
      int16_t year = sql::ValUtil::ExtractYear(date);
      uint8_t month = sql::ValUtil::ExtractMonth(date);
      uint8_t day = sql::ValUtil::ExtractDay(date);
      return codegen_->DateToSql(year, month, day);
    }
    case terrier::type::TypeId::TIMESTAMP:
      // TODO(tanujnay112): add AST support for these
      return nullptr;
    case terrier::type::TypeId::DECIMAL:
      return codegen_->FloatToSql(terrier::type::TransientValuePeeker::PeekDecimal(trans_val));
    case terrier::type::TypeId::VARCHAR:
      return codegen_->StringToSql(terrier::type::TransientValuePeeker::PeekVarChar(trans_val));
    case terrier::type::TypeId::VARBINARY:
      // TODO(Amadou): Add AST support for this
      return nullptr;
    default:
      return nullptr;
  }
}
};  // namespace terrier::execution::compiler
