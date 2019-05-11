#include "execution/compiler/expression/tuple_value_translator.h"
#include "parser/expression/tuple_value_expression.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/code_context.h"

namespace tpl::compiler {
TupleValueTranslator::TupleValueTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext &context)
    : ExpressionTranslator(expression, context) {};

ast::Expr *TupleValueTranslator::DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch &row) {
  auto tuple_val = GetExpressionAs<terrier::parser::TupleValueExpression>();
  auto codegen = context_.GetCodeGen();
  // Use the region allocator because the identifier will outlive this scope.
  char* col_name = codegen->GetRegion()->AllocateArray<char>(tuple_val.GetColumnName().size() + 1); //tuple_val.GetColumnName();
  std::memcpy(col_name, tuple_val.GetColumnName().c_str(),tuple_val.GetColumnName().size() + 1);
  auto col_ident = (*codegen)->NewIdentifierExpr(DUMMY_POS, codegen->GetCodeContext()->GetAstContext()->GetIdentifier(col_name));
  return (*codegen)->NewMemberExpr(DUMMY_POS, row.GetIdentifierExpr(), col_ident);
}
};