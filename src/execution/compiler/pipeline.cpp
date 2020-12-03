#include "execution/compiler/pipeline.h"

#include <algorithm>

#include "common/macros.h"
#include "common/settings.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query_builder.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline_driver.h"
#include "execution/compiler/work_context.h"
#include "execution/exec/execution_settings.h"
#include "loggers/execution_logger.h"
#include "metrics/metrics_defs.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

query_id_t Pipeline::GetQueryId() const { return compilation_context_->GetQueryId(); }

Pipeline::Pipeline(CompilationContext *ctx)
    : id_(ctx->RegisterPipeline(this)),
      compilation_context_(ctx),
      codegen_(compilation_context_->GetCodeGen()),
      driver_(nullptr),
      parallelism_(Parallelism::Parallel),
      check_parallelism_(true),
      state_var_(codegen_->MakeIdentifier("pipelineState")),
      state_(codegen_->MakeIdentifier(fmt::format("P{}_State", id_)),
             [this](CodeGen *codegen) { return codegen_->MakeExpr(state_var_); }) {}

Pipeline::Pipeline(OperatorTranslator *op, Pipeline::Parallelism parallelism) : Pipeline(op->GetCompilationContext()) {
  UpdateParallelism(parallelism);
  RegisterStep(op);
}

void Pipeline::RegisterStep(OperatorTranslator *op) {
  NOISEPAGE_ASSERT(std::count(steps_.begin(), steps_.end(), op) == 0,
                   "Duplicate registration of operator in pipeline.");
  auto num_steps = steps_.size();
  if (num_steps > 0) {
    auto last_step = common::ManagedPointer(steps_[num_steps - 1]);
    // TODO(WAN): MAYDAY CHECK WITH LIN AND PRASHANTH, did ordering of these change?
    last_step->SetChildTranslator(common::ManagedPointer(op));
    op->SetParentTranslator(last_step);
  }
  steps_.push_back(op);
}

void Pipeline::RegisterSource(PipelineDriver *driver, Pipeline::Parallelism parallelism) {
  driver_ = driver;
  UpdateParallelism(parallelism);
}

void Pipeline::UpdateParallelism(Pipeline::Parallelism parallelism) {
  if (check_parallelism_) {
    parallelism_ = std::min(parallelism, parallelism_);
  }
}

void Pipeline::SetParallelCheck(bool check) { check_parallelism_ = check; }

void Pipeline::RegisterExpression(ExpressionTranslator *expression) {
  NOISEPAGE_ASSERT(std::find(expressions_.begin(), expressions_.end(), expression) == expressions_.end(),
                   "Expression already registered in pipeline");
  expressions_.push_back(expression);
}

StateDescriptor::Entry Pipeline::DeclarePipelineStateEntry(const std::string &name, ast::Expr *type_repr) {
  return state_.DeclareStateEntry(codegen_, name, type_repr);
}

std::string Pipeline::CreatePipelineFunctionName(const std::string &func_name) const {
  auto result = fmt::format("{}_Pipeline{}", compilation_context_->GetFunctionPrefix(), id_);
  if (!func_name.empty()) {
    result += "_" + func_name;
  }
  return result;
}

ast::Identifier Pipeline::GetSetupPipelineStateFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("InitPipelineState"));
}

ast::Identifier Pipeline::GetTearDownPipelineStateFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDownPipelineState"));
}

ast::Identifier Pipeline::GetWorkFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName(IsParallel() ? "ParallelWork" : "SerialWork"));
}

void Pipeline::InjectStartResourceTracker(FunctionBuilder *builder, bool is_hook) const {
  if (compilation_context_->IsPipelineMetricsEnabled()) {
    auto *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();

    // Initialize the feature vector, register and start tracker
    std::vector<ast::Expr *> args{exec_ctx, oufeatures_.GetPtr(codegen_),
                                  codegen_->Const64(GetPipelineId().UnderlyingValue()), codegen_->ConstBool(is_hook)};
    auto call = codegen_->CallBuiltin(ast::Builtin::ExecOUFeatureVectorInitialize, args);
    builder->Append(codegen_->MakeStmt(call));

    args = {exec_ctx};
    call = codegen_->CallBuiltin(ast::Builtin::RegisterThreadWithMetricsManager, args);
    builder->Append(codegen_->MakeStmt(call));

    // Inject StartPipelineTracker()
    args = {exec_ctx, codegen_->Const64(GetPipelineId().UnderlyingValue())};
    auto start_call = codegen_->CallBuiltin(ast::Builtin::ExecutionContextStartPipelineTracker, args);
    builder->Append(codegen_->MakeStmt(start_call));
  }
}

void Pipeline::InjectEndResourceTracker(FunctionBuilder *builder, bool is_hook) const {
  if (compilation_context_->IsPipelineMetricsEnabled()) {
    auto *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();

    // Inject EndPipelineTracker();
    std::vector<ast::Expr *> args = {exec_ctx};
    args.push_back(codegen_->Const64(GetQueryId().UnderlyingValue()));
    args.push_back(codegen_->Const64(GetPipelineId().UnderlyingValue()));
    args.push_back(oufeatures_.GetPtr(codegen_));
    auto end_call = codegen_->CallBuiltin(ast::Builtin::ExecutionContextEndPipelineTracker, args);
    builder->Append(codegen_->MakeStmt(end_call));

    // Aggregate
    if (IsParallel()) {
      std::vector<ast::Expr *> args{exec_ctx};
      auto call = codegen_->CallBuiltin(ast::Builtin::AggregateMetricsThread, args);
      builder->Append(codegen_->MakeStmt(call));
    }

    // Reset the pipeline features
    args = {oufeatures_.GetPtr(codegen_)};
    auto call = codegen_->CallBuiltin(ast::Builtin::ExecOUFeatureVectorReset, args);
    builder->Append(codegen_->MakeStmt(call));
  }
}

util::RegionVector<ast::FieldDecl *> Pipeline::PipelineParams() const {
  // The main query parameters.
  util::RegionVector<ast::FieldDecl *> query_params = compilation_context_->QueryParams();
  // Tag on the pipeline state.
  ast::Expr *pipeline_state = codegen_->PointerType(codegen_->MakeExpr(state_.GetTypeName()));
  query_params.push_back(codegen_->MakeField(state_var_, pipeline_state));
  return query_params;
}

void Pipeline::LinkSourcePipeline(Pipeline *dependency) {
  NOISEPAGE_ASSERT(dependency != nullptr, "Source cannot be null");
  dependencies_.push_back(dependency);
}

void Pipeline::CollectDependencies(std::vector<Pipeline *> *deps) {
  for (auto *pipeline : dependencies_) {
    pipeline->CollectDependencies(deps);
  }
  deps->push_back(this);
}

void Pipeline::Prepare(const exec::ExecutionSettings &exec_settings) {
  // Finalize the pipeline state.
  if (compilation_context_->IsPipelineMetricsEnabled()) {
    ast::Expr *type = codegen_->BuiltinType(ast::BuiltinType::ExecOUFeatureVector);
    oufeatures_ = DeclarePipelineStateEntry("execFeatures", type);
  }
  state_.ConstructFinalType(codegen_);

  // Finalize the execution mode. We choose serial execution if ANY of the below
  // conditions are satisfied:
  //  1. If parallel execution is globally disabled.
  //  2. If the consumer doesn't support parallel execution.
  //  3. If ANY operator in the pipeline explicitly requested serial execution.

  const bool parallel_exec_disabled = !exec_settings.GetIsParallelQueryExecutionEnabled();
  const bool parallel_consumer = true;
  if (parallel_exec_disabled || !parallel_consumer || parallelism_ == Pipeline::Parallelism::Serial) {
    parallelism_ = Pipeline::Parallelism::Serial;
  } else {
    parallelism_ = Pipeline::Parallelism::Parallel;
  }

  // Pretty print.
  {
    std::string result;
    bool first = true;
    for (auto iter = Begin(), end = End(); iter != end; ++iter) {
      if (!first) result += " --> ";
      first = false;
      std::string plan_type = planner::PlanNodeTypeToString((*iter)->GetPlan().GetPlanNodeType());
      std::transform(plan_type.begin(), plan_type.end(), plan_type.begin(), ::tolower);
      result.append(plan_type);
    }
    EXECUTION_LOG_TRACE("Pipeline-{}: parallel={}, vectorized={}, steps=[{}]", id_, IsParallel(), IsVectorized(),
                        result);
  }
}

ast::FunctionDecl *Pipeline::GenerateSetupPipelineStateFunction() const {
  auto name = GetSetupPipelineStateFunctionName();
  FunctionBuilder builder(codegen_, name, PipelineParams(), codegen_->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen_);
    for (auto *op : steps_) {
      op->InitializePipelineState(*this, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineStateFunction() const {
  auto name = GetTearDownPipelineStateFunctionName();
  FunctionBuilder builder(codegen_, name, PipelineParams(), codegen_->Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(codegen_);
    for (auto *op : steps_) {
      op->TearDownPipelineState(*this, &builder);
    }

    if (compilation_context_->IsPipelineMetricsEnabled()) {
      // Reset the pipeline features
      auto args = {oufeatures_.GetPtr(codegen_)};
      auto call = codegen_->CallBuiltin(ast::Builtin::ExecOUFeatureVectorReset, args);
      builder.Append(codegen_->MakeStmt(call));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction() const {
  auto query_state = compilation_context_->GetQueryState();
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Init"));
  FunctionBuilder builder(codegen_, name, compilation_context_->QueryParams(), codegen_->Nil());
  {
    CodeGen::CodeScope code_scope(codegen_);
    // var tls = @execCtxGetTLS(exec_ctx)
    ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
    ast::Identifier tls = codegen_->MakeFreshIdentifier("threadStateContainer");
    builder.Append(codegen_->DeclareVarWithInit(tls, codegen_->ExecCtxGetTLS(exec_ctx)));
    // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
    ast::Expr *state_ptr = query_state->GetStatePointer(codegen_);
    builder.Append(codegen_->TLSReset(codegen_->MakeExpr(tls), state_.GetTypeName(),
                                      GetSetupPipelineStateFunctionName(), GetTearDownPipelineStateFunctionName(),
                                      state_ptr));
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction() const {
  auto params = PipelineParams();

  if (IsParallel()) {
    auto additional_params = driver_->GetWorkerParams();
    params.insert(params.end(), additional_params.begin(), additional_params.end());
  }

  FunctionBuilder builder(codegen_, GetWorkFunctionName(), std::move(params), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);
    if (IsParallel()) {
      for (auto *op : steps_) {
        op->BeginParallelPipelineWork(*this, &builder);
      }

      InjectStartResourceTracker(&builder, false);
    }

    // Create the working context and push it through the pipeline.
    WorkContext context(compilation_context_, *this);
    (*Begin())->PerformPipelineWork(&context, &builder);

    if (IsParallel()) {
      for (auto *op : steps_) {
        op->EndParallelPipelineWork(*this, &builder);
      }

      InjectEndResourceTracker(&builder, false);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction() const {
  bool started_tracker = false;
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Run"));
  FunctionBuilder builder(codegen_, name, compilation_context_->QueryParams(), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);

    // TODO(abalakum): This shouldn't actually be dependent on order and the loop can be simplified
    // after issue #1154 is fixed
    // Let the operators perform some initialization work in this pipeline.
    for (auto iter = Begin(), end = End(); iter != end; ++iter) {
      (*iter)->BeginPipelineWork(*this, &builder);
    }

    // var pipelineState = @tlsGetCurrentThreadState(...)
    auto exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
    auto tls = codegen_->ExecCtxGetTLS(exec_ctx);
    auto state = codegen_->TLSAccessCurrentThreadState(tls, state_.GetTypeName());
    builder.Append(codegen_->DeclareVarWithInit(state_var_, state));

    // Launch pipeline work.
    if (IsParallel()) {
      driver_->LaunchWork(&builder, GetWorkFunctionName());
    } else {
      // SerialWork(queryState, pipelineState)
      InjectStartResourceTracker(&builder, false);
      started_tracker = true;

      builder.Append(
          codegen_->Call(GetWorkFunctionName(), {builder.GetParameterByPosition(0), codegen_->MakeExpr(state_var_)}));
    }

    // TODO(abalakum): This shouldn't actually be dependent on order and the loop can be simplified
    // after issue #1154 is fixed
    // Let the operators perform some completion work in this pipeline.
    for (auto iter = Begin(), end = End(); iter != end; ++iter) {
      (*iter)->FinishPipelineWork(*this, &builder);
    }

    if (started_tracker) {
      InjectEndResourceTracker(&builder, false);
    }
  }

  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction() const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDown"));
  FunctionBuilder builder(codegen_, name, compilation_context_->QueryParams(), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);
    // Tear down thread local state if parallel pipeline.
    ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
    builder.Append(codegen_->TLSClear(codegen_->ExecCtxGetTLS(exec_ctx)));

    auto call = codegen_->CallBuiltin(ast::Builtin::EnsureTrackersStopped, {exec_ctx});
    builder.Append(codegen_->MakeStmt(call));
  }
  return builder.Finish();
}

void Pipeline::GeneratePipeline(ExecutableQueryFragmentBuilder *builder) const {
  // Declare the pipeline state.
  builder->DeclareStruct(state_.GetType());

  // Generate pipeline state initialization and tear-down functions.
  builder->DeclareFunction(GenerateSetupPipelineStateFunction());
  builder->DeclareFunction(GenerateTearDownPipelineStateFunction());

  // Generate main pipeline logic.
  builder->DeclareFunction(GeneratePipelineWorkFunction());

  // Register the main init, run, tear-down functions as steps, in that order.
  builder->RegisterStep(GenerateInitPipelineFunction());
  builder->RegisterStep(GenerateRunPipelineFunction());
  auto teardown = GenerateTearDownPipelineFunction();
  builder->RegisterStep(teardown);
  builder->AddTeardownFn(teardown);
}

}  // namespace noisepage::execution::compiler
