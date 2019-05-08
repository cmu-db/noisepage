#include "execution/compiler/expression/tuple_value_translator.h"
#include "parser/expression/tuple_value_expression.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {
TupleValueTranslator::TupleValueTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext &context)
    : ExpressionTranslator(expression, context) {};

ast::Expr *TupleValueTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) {
  auto tuple_val = GetExpressionAs<terrier::parser::TupleValueExpression>();
  auto col_name = tuple_val.GetTableName();
  auto codegen = context_.GetCodeGen();
  auto col_ident = (*codegen)->NewIdentifierExpr(DUMMY_POS, ast::Identifier(col_name.c_str()));
  return (*codegen)->NewMemberExpr(DUMMY_POS, row.GetIdentifierExpr(), col_ident);
}
};