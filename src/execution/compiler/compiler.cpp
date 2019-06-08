#include "execution/compiler/compiler.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/operator/operator_state.h"

namespace tpl::compiler {

Compiler::Compiler(tpl::compiler::Query *query) : query_(query), consumer_(query_->GetFinalSchema()) {
  // Make the pipelines
  Pipeline * main_pipeline = new (query_->GetRegion()) Pipeline();
  MakePipelines(query_->GetPlan(), main_pipeline);
}

void Compiler::Compile() {
  
}

void Compiler::MakePipelines(const terrier::planner::AbstractPlanNode & op, Pipeline * curr_pipeline) {
  switch (op.GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
    case terrier::planner::PlanNodeType::ORDERBY: {
      OperatorState * op_state = new (query_->GetRegion()) OperatorState(op);
      OperatorTranslator *top_translator = TranslatorFactory::CreateTopTranslator(op, op_state);
      OperatorTranslator *bottom_translator = TranslatorFactory::CreateBottomTranslator(op, op_state);
      curr_pipeline->Add(top_translator);
      Pipeline *next_pipeline = new(query_->GetRegion()) Pipeline();
      next_pipeline->Add(bottom_translator);
      MakePipelines(*op.GetChild(0), next_pipeline);
      pipelines_.push_back(curr_pipeline);
      return;
    }
    case terrier::planner::PlanNodeType::NESTLOOP :
    case terrier::planner::PlanNodeType::HASHJOIN: {
      // Create left and right translators
      OperatorTranslator * left_translator = TranslatorFactory::CreateLeftTranslator(op, op_state);
      OperatorTranslator * right_translator = TranslatorFactory::CreateRightTranslator(op, op_state);
      // Generate left pipeline
      Pipeline *next_pipeline = new(query_->GetRegion()) Pipeline();
      next_pipeline->Add(left_translator);
      MakePipelines(*op.GetChild(0), next_pipeline);
      // Continue right pipeline
      curr_pipeline->Add(right_translator);
      MakePipelines(*op.GetChild(1), curr_pipeline);
      return;
    }
    default: {
      OperatorTranslator *translator = TranslatorFactory::CreateRegularTranslator(op, op_state);
      curr_pipeline->Add(translator);
      if (op.GetChildrenSize() != 0) MakePipelines(*op.GetChild(0), curr_pipeline);
      pipelines_.push_back(curr_pipeline);
      return;
    }
  }
}
}

