#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/compilation_context.h"
#include "parser/expression/constant_value_expression.h"
#include "type/transient_value_peeker.h"

namespace tpl::compiler {
ConstantTranslator::ConstantTranslator(const terrier::parser::AbstractExpression *expression,
                                       CodeGen * codegen)
    : ExpressionTranslator(expression, codegen) {}

ast::Expr *ConstantTranslator::DeriveExpr(OperatorTranslator * translator) {
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
      // TODO(tanujnay112): add AST support for these
      return codegen_->IntToSql(static_cast<int32_t>(terrier::type::TransientValuePeeker::PeekBigInt(trans_val)));
    case terrier::type::TypeId::BOOLEAN:
      // TODO(Amadou): Convert these to sql types if necessary
      return codegen_->BoolLiteral(terrier::type::TransientValuePeeker::PeekBoolean(trans_val));
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
      // TODO(tanujnay112): add AST support for these
      return nullptr;
    case terrier::type::TypeId::DECIMAL:
      // TODO(tanujnay112): add AST support for these
      return codegen_->FloatLiteral(static_cast<float_t>(terrier::type::TransientValuePeeker::PeekDecimal(trans_val)));
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
      // TODO(Amadou): Add AST support for this
      return nullptr;
    default:
      return nullptr;
  }
}
};  // namespace tpl::compiler
