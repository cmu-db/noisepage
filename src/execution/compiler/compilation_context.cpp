#include "execution/compiler/compilation_context.h"

#include "execution/compiler/code_context.h"

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/query.h"
#include "execution/compiler/query_state.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

CompilationContext::CompilationContext(Query *query, ExecutionConsumer *consumer)
    : query_(query), consumer_(consumer), codegen_(query_->GetCodeContext()) {}

void CompilationContext::GeneratePlan(Query *query) {
  Pipeline main_pipeline(this);
  consumer_->Prepare(this);
  Prepare(query->GetPlan(), &main_pipeline);
  query->GetQueryState()->FinalizeType(&codegen_);

  util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
  params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, ast::Identifier(query->GetQueryStateName().c_str()), query->GetQueryState()->GetType()));
  FunctionBuilder init_fn(codegen_, ast::Identifier(query->GetQueryInitName().c_str()), std::move(params), codegen_.Ty_Nil());

//  consumer_->InitializeQueryState(this);
  for (const auto &it : op_translators_) {
    it.second->InitializeQueryState();
  }
  init_fn.Finish();


}

void CompilationContext::Prepare(const terrier::planner::AbstractPlanNode &op, tpl::compiler::Pipeline *pipeline) {
  op_translators_.emplace(std::make_pair(op, translator_factory_.CreateTranslator(op, pipeline)));
}

void CompilationContext::Prepare(const terrier::parser::AbstractExpression &ex) {
  ex_translators_.emplace(std::make_pair(ex, translator_factory_.CreateTranslator(ex)));
}

}