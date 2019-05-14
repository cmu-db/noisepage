#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

  UnaryTranslator::UnaryTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context)
      : ExpressionTranslator(expression, context) {
    context->Prepare(*expression->GetChild(0));
  };

  ast::Expr *UnaryTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) {
    auto *left = row->DeriveValue(*expression->GetChild(0));
    parsing::Token::Type type;
    switch(expression->GetExpressionType()){
      case terrier::parser::ExpressionType::OPERATOR_UNARY_MINUS:
        type = parsing::Token::Type::MINUS;
        break;
      case terrier::parser::ExpressionType::OPERATOR_NOT:
        type = parsing::Token::Type::BANG;
        break;
      default:
        TPL_ASSERT(false, "Unsupported expression");
    }
    return (*context_->GetCodeGen())->NewUnaryOpExpr(DUMMY_POS, type, left);
  }
}