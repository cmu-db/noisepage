#include "execution/compiler/expression/function_translator.h"

#include <type_traits>

#include "catalog/catalog_accessor.h"
#include "common/managed_pointer.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/translator_factory.h"
#include "execution/udf/udf_context.h"
#include "execution/util/execution_common.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/function_expression.h"

namespace terrier {
namespace execution {
namespace ast {
class Expr;
}  // namespace ast
}  // namespace execution
}  // namespace terrier

namespace terrier::execution::compiler {
FunctionTranslator::FunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {
  for (auto child : expression->GetChildren()) {
    params_.push_back(TranslatorFactory::CreateExpressionTranslator(child.Get(), codegen_));
  }
}

ast::Expr *FunctionTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto func_expr = GetExpressionAs<parser::FunctionExpression>();
  auto proc_oid = func_expr->GetProcOid();
  auto udf_ctx = codegen_->Accessor()->GetUDFContext(proc_oid);
  if (!udf_ctx->IsBuiltin()) {
    UNREACHABLE("We don't support non-builtin UDF's yet!");
  }

  std::vector<ast::Expr *> params;
  if (udf_ctx->IsExecCtxRequired()) {
    params.push_back(codegen_->MakeExpr(codegen_->GetExecCtxVar()));
  }
  for (auto &param : params_) {
    params.push_back(param->DeriveExpr(evaluator));
  }

  return codegen_->BuiltinCall(udf_ctx->GetBuiltin(), std::move(params));
}
};  // namespace terrier::execution::compiler
