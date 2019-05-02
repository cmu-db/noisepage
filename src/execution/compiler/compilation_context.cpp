#include "execution/compiler/compilation_context.h"

#include "execution/compiler/code_context.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/query.h"
#include "execution/compiler/query_state.h"

namespace tpl::compiler {

CompilationContext::CompilationContext(tpl::compiler::Query *query, tpl::compiler::ExecutionConsumer *consumer)
: codegen_(&query->GetCodeContext())
{

}

void CompilationContext::Prepare(const terrier::parser::AbstractExpression &expr) {
  ex_translators_.insert(std::make_pair(&expr, translator_factory_.CreateTranslator(expr, this)));
}

void CompilationContext::Prepare(const terrier::planner::AbstractPlanNode &node, Pipeline *pipeline) {
  op_translators_.insert(std::make_pair(&node, translator_factory_.CreateTranslator(node, this, pipeline)));
}

ExpressionTranslator *CompilationContext::GetTranslator(const terrier::parser::AbstractExpression &expr) const {
  auto iter = ex_translators_.find(&expr);
  return iter == ex_translators_.end() ? nullptr : iter->second.get();
}

OperatorTranslator *CompilationContext::GetTranslator(const terrier::planner::AbstractPlanNode &node) const {
  auto iter = op_translators_.find(&node);
  return iter == op_translators_.end() ? nullptr : iter->second.get();
}

void CompilationContext::Produce(const terrier::planner::AbstractPlanNode &node) {
  GetTranslator(node)->Produce();
}

void CompilationContext::GeneratePlan(Query *query) {
  Pipeline main_pipeline(this);
  consumer_->Prepare(this);
  Prepare(query->GetPlan(), &main_pipeline);
  query->GetQueryState().FinalizeType(&codegen_);

  // TODO(WAN): init, plan, teardown
  std::shared_ptr<ast::BlockStmt> compiled_query;
  // init
  std::vector<FunctionDec> args = {{"query_state", query->GetQueryState().GetType()}};
  FunctionBuilder init_func(query->GetCodeContext(), "init", codegen_.Ty_Nil(), args);
  consumer_->InitializeQueryState(this);
  for (const auto &iter : op_translators_) {
    iter.second->InitializeQueryState();
  }


  // plan

  // teardown
  consumer_->TeardownQueryState(this);


  query->SetCompiledQuery(compiled_query);
}

}
