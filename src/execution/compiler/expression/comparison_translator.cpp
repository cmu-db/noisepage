#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

  ComparisonTranslator::ComparisonTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext &context)
      : ExpressionTranslator(expression, context) {
    context.Prepare(*expression->GetChild(0));
    context.Prepare(*expression->GetChild(1));
  };

  ast::Expr *ComparisonTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) {
    auto *right = row.DeriveValue(*expression->GetChild(0));
    auto *left = row.DeriveValue(*expression->GetChild(1));
    parsing::Token::Type type;
    switch(expression->GetExpressionType()){
      case terrier::parser::ExpressionType::COMPARE_EQUAL:
        type = parsing::Token::Type::EQUAL_EQUAL;
        break;
      case terrier::parser::ExpressionType::COMPARE_GREATER_THAN:
        type = parsing::Token::Type::GREATER;
        break;
      case terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
        type = parsing::Token::Type::GREATER_EQUAL;
        break;
      case terrier::parser::ExpressionType::COMPARE_LESS_THAN:
        type = parsing::Token::Type::LESS;
        break;
      case terrier::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
        type = parsing::Token::Type::LESS_EQUAL;
        break;
      default:
        TPL_ASSERT(false, "Unsupported expression");
    }
    return (*context_.GetCodeGen())->NewComparisonOpExpr(DUMMY_POS, type, left, right);
  }
}