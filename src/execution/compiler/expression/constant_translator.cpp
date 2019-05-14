#include "type/transient_value_peeker.h"
#include "execution/compiler/expression/constant_translator.h"
#include "parser/expression/constant_value_expression.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {
  ConstantTranslator::ConstantTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context)
      : ExpressionTranslator(expression, context) {};

  ast::Expr *ConstantTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) {
    auto const_val = GetExpressionAs<terrier::parser::ConstantValueExpression>();
    auto trans_val = const_val.GetValue();
    auto codegen = context_->GetCodeGen();
    auto type = trans_val.Type();
    switch (type) {
      case terrier::type::TypeId::TINYINT:
        return (*codegen)->NewIntLiteral(DUMMY_POS, terrier::type::TransientValuePeeker::PeekTinyInt(trans_val));
      case terrier::type::TypeId::SMALLINT:
        return (*codegen)->NewIntLiteral(DUMMY_POS, terrier::type::TransientValuePeeker::PeekSmallInt(trans_val));
      case terrier::type::TypeId::INTEGER:
        return (*codegen)->NewIntLiteral(DUMMY_POS, terrier::type::TransientValuePeeker::PeekInteger(trans_val));
      case terrier::type::TypeId::BIGINT:
        // TODO(tanujnay112): add AST support for these
        return (*codegen)->NewIntLiteral(DUMMY_POS,
            static_cast<int32_t>(terrier::type::TransientValuePeeker::PeekBigInt(trans_val)));
      case terrier::type::TypeId::BOOLEAN:
        return (*codegen)->NewBoolLiteral(DUMMY_POS, terrier::type::TransientValuePeeker::PeekBoolean(trans_val));
      case terrier::type::TypeId::DATE:
      case terrier::type::TypeId::TIMESTAMP:
        // TODO(tanujnay112): add AST support for these
        return nullptr;
      case terrier::type::TypeId::DECIMAL:
        // TODO(tanujnay112): add AST support for these
        return (*codegen)->NewFloatLiteral(DUMMY_POS,
            static_cast<float_t >(terrier::type::TransientValuePeeker::PeekDecimal(trans_val)));
      case terrier::type::TypeId::VARCHAR:
      case terrier::type::TypeId::VARBINARY:
        return (*codegen)->NewStringLiteral(DUMMY_POS,
            ast::Identifier(terrier::type::TransientValuePeeker::PeekVarChar(trans_val)));
      default:
        return nullptr;
    }
  }
};