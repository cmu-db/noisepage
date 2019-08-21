#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

ConjunctionTranslator::ConjunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen),
      left_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(0).get(), codegen_)),
      right_(TranslatorFactory::CreateExpressionTranslator(expression_->GetChild(1).get(), codegen_)) {}

ast::Expr *ConjunctionTranslator::DeriveExpr(OperatorTranslator *translator) {
  auto *left_expr = left_->DeriveExpr(translator);
  auto *right_expr = right_->DeriveExpr(translator);
  parsing::Token::Type type;
  switch (expression_->GetExpressionType()) {
    case terrier::parser::ExpressionType::CONJUNCTION_OR:
      // @Wan so the issue here is that the factory takes in the argument for left and right child. This implies that
      // either we recursively obtain those children and pass it to this Translate function via a vector or we edit the
      // nodes to have setters/make ourselves friends
      type = parsing::Token::Type::OR;
      break;
    case terrier::parser::ExpressionType::CONJUNCTION_AND:
      type = parsing::Token::Type::AND;
      break;
    default:
      UNREACHABLE("Unsupported expression");
  }
  return codegen_->BinaryOp(type, left_expr, right_expr);
}
}  // namespace terrier::execution::compiler
