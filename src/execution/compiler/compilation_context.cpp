#include "execution/compiler/compilation_context.h"

#include "execution/compiler/code_context.h"

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/query.h"
#include "execution/compiler/query_state.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

CompilationContext::CompilationContext(CodeContext *code_ctx, QueryState *query_state, ExecutionConsumer *consumer)
    : code_ctx_(code_ctx), query_state_(query_state), consumer_(consumer), codegen_(code_ctx_) {}

void CompilationContext::GeneratePlan(Query *query) {
  Pipeline main_pipeline(this);
  consumer_->Prepare(this);
  Prepare(query->GetPlan(), &main_pipeline);
  query_state_->FinalizeType(&codegen_);

  // todo(wan): this wraps into function_builder
  util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
  params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, ast::Identifier(query->GetQueryStateName().c_str()), query_state_->GetType()));
  auto fn_ty = codegen_->NewFunctionType(DUMMY_POS, std::move(params), codegen_.Ty_Nil());
  auto fn_lit = codegen_->NewFunctionLitExpr(fn_ty, codegen_.EmptyBlock());
  auto fn_decl = codegen_->NewFunctionDecl(DUMMY_POS, ast::Identifier(query->GetQueryInitName().c_str()), fn_lit);
  consumer_->InitializeQueryState(this);
  for (const auto &it : op_translators_) {
    it.second->InitializeQueryState();
  }

}

void CompilationContext::Prepare(const terrier::planner::AbstractPlanNode &op, tpl::compiler::Pipeline *pipeline) {
  op_translators_.emplace(std::make_pair(op, translator_factory_.CreateTranslator(op, pipeline)));
}

void CompilationContext::Prepare(const terrier::parser::AbstractExpression &ex) {
  ex_translators_.emplace(std::make_pair(ex, translator_factory_.CreateTranslator(ex)));
}

}