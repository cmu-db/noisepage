#include <planner/plannodes/seq_scan_plan_node.h>
#include "execution/compiler/operator/seq_scan_translator.h"

#include "execution/compiler/consumer_context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/row_batch.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

SeqScanTranslator::SeqScanTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline)
    : OperatorTranslator(op, pipeline) {
  pipeline->GetCompilationContext()
  ->Prepare(*GetOperatorAs<terrier::planner::SeqScanPlanNode>().GetScanPredicate());
}

void SeqScanTranslator::Produce() {
  CodeGen *codegen = pipeline_->GetCodeGen();
  auto row_id = (*codegen)->NewIdentifierExpr(DUMMY_POS, codegen->NewIdentifier());
  RowBatch row_batch(*pipeline_->GetCompilationContext(), row_id);

  auto target = row_batch.GetIdentifierExpr();
  auto table_name = (*codegen)->NewIdentifierExpr(DUMMY_POS, ast::Identifier("table_1"));
  auto current_fn = codegen->GetCurrentFunction();
  current_fn->StartForInStmt(target, table_name, nullptr);
  auto predicate = GetOperatorAs<terrier::planner::SeqScanPlanNode>().GetScanPredicate();
  auto predicate_expr = pipeline_->GetCompilationContext()
      ->GetTranslator(*predicate)->DeriveExpr(predicate.get(), row_batch);
  current_fn->StartIfStmt(predicate_expr);
  ConsumerContext ctx(pipeline_->GetCompilationContext(), pipeline_);
  ctx.Consume(&row_batch);
}

} // namespace tpl::compiler