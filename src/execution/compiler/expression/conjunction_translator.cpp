#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/codegen.h"

namespace tpl::compiler {

  ConjunctionTranslator::ConjunctionTranslator(const terrier::parser::AbstractExpression *expression,
      CompilationContext &context) : ExpressionTranslator(expression, context) {
    context.Prepare(*expression->GetChild(0));
    context.Prepare(*expression->GetChild(1));
  }

  ast::Expr *ConjunctionTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) {
    auto *left = row.DeriveValue(*expression->GetChild(0));
    auto *right = row.DeriveValue(*expression->GetChild(1));
    parsing::Token::Type type;
    switch(expression->GetExpressionType()){
      case terrier::parser::ExpressionType::CONJUNCTION_OR:
        //@Wan so the issue here is that the factory takes in the argument for left and right child. This implies that
        //either we recursively obtain those children and pass it to this Translate function via a vector or we edit the
        //nodes to have setters/make ourselves friends
        type = parsing::Token::Type::OR;
        break;
      case terrier::parser::ExpressionType::CONJUNCTION_AND:
        type = parsing::Token::Type::AND;
        break;
      default:
        TPL_ASSERT(false, "Unsupported expression");
    }
    return (*context_.GetCodeGen())->NewBinaryOpExpr(DUMMY_POS, type, left, right);
  }
}