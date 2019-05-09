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

  // This doesn't recursively call on the rest of the nodes?
  Prepare(query->GetPlan(), &main_pipeline);
  query->GetQueryState()->FinalizeType(&codegen_);

  util::RegionVector<ast::Stmt *> stmts(query->GetRegion());
  ast::Identifier qs_id(query->GetQueryStateName().c_str());
  auto qs_type = query->GetQueryState()->GetType();

  {
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type));
    FunctionBuilder
        init_fn(codegen_, ast::Identifier(query->GetQueryInitName().c_str()), std::move(params), codegen_.Ty_Nil());
    consumer_->InitializeQueryState(this);
    for (const auto &it : op_translators_) {
      it.second->InitializeQueryState();
    }
    stmts.emplace_back(codegen_->NewDeclStmt(init_fn.Finish()));
  }

  {
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type));
    FunctionBuilder
        produce_fn(codegen_, ast::Identifier(query->GetQueryProduceName().c_str()), std::move(params), codegen_.Ty_Nil());
    GetTranslator(query->GetPlan())->Produce();
    stmts.emplace_back(codegen_->NewDeclStmt(produce_fn.Finish()));
  }

  {
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type));
    FunctionBuilder
        teardown_fn(codegen_, ast::Identifier(query->GetQueryTeardownName().c_str()), std::move(params), codegen_.Ty_Nil());
    consumer_->TeardownQueryState(this);
    for (const auto &it : op_translators_) {
      it.second->TeardownQueryState();
    }
    stmts.emplace_back(codegen_->NewDeclStmt(teardown_fn.Finish()));
  }

  const auto compiled_fn = codegen_->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
  (void) compiled_fn;
  query->SetCompiledFunction(compiled_fn);
}

u32 CompilationContext::RegisterPipeline(Pipeline *pipeline) {
  auto pos = pipelines_.size();
  pipelines_.emplace_back(pipeline);
  return pos;
}

ExecutionConsumer *CompilationContext::GetExecutionConsumer() { return consumer_; }
CodeGen *CompilationContext::GetCodeGen() { return &codegen_; }

util::Region *CompilationContext::GetRegion() {
  return query_->GetRegion();
}

void CompilationContext::Prepare(const terrier::planner::AbstractPlanNode &op, tpl::compiler::Pipeline *pipeline) {
  op_translators_.emplace(std::make_pair(&op, translator_factory_.CreateTranslator(op, pipeline)));
}

// Prepare the translator for the given expression
void CompilationContext::Prepare(const terrier::parser::AbstractExpression &exp) {
  auto translator = translator_factory_.CreateTranslator(exp, *this);
  ex_translators_.insert(std::make_pair(&exp, std::move(translator)));
}

// Get the registered translator for the given operator
OperatorTranslator *CompilationContext::GetTranslator(const terrier::planner::AbstractPlanNode &op) const {
  auto iter = op_translators_.find(&op);
  return iter == op_translators_.end() ? nullptr : iter->second;
}

ExpressionTranslator *CompilationContext::GetTranslator(const terrier::parser::AbstractExpression &ex) const {
  auto iter = ex_translators_.find(&ex);
  return iter == ex_translators_.end() ? nullptr : iter->second;
}

}