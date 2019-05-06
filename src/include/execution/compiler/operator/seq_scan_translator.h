#pragma once

#include <execution/compiler/compiler_defs.h>
#include "execution/compiler/code_context.h"
#include "execution/compiler/consumer_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"

#include "planner/plannodes/abstract_scan_plan_node.h"

namespace tpl::compiler {

class SeqScanTranslator : public OperatorTranslator{

 public:

  SeqScanTranslator(const terrier::planner::AbstractPlanNode &planNode, Pipeline *pipeline)
  : OperatorTranslator(planNode, pipeline) {}

  void Produce() override {
    CodeGen &codegen = pipeline_->GetCodeGen();
    auto *outer = new CodeBlock();
    RowBatch rowBatch(codegen);

    auto target = rowBatch.GetName();
    auto table_name = codegen->NewIdentifierExpr(DUMMY_POS, ast::Identifier("table_1"));
    ConsumerContext ctx(pipeline_->GetCompilationContext(), pipeline_);
    ctx.Consume(rowBatch);

    auto for_in = codegen->NewForInStmt(DUMMY_POS, target, table_name, nullptr, inner_block);
    return outer;
  }
};


} // namespace tpl::compiler