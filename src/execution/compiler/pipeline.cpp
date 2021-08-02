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
#include "planner/plannodes/output_schema.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

query_id_t Pipeline::GetQueryId() const { return compilation_context_->GetQueryId(); }

Pipeline::Pipeline(CompilationContext *ctx)
    : id_(ctx->RegisterPipeline(this)),
      compilation_context_(ctx),
      codegen_(compilation_context_->GetCodeGen()),
      state_(codegen_->MakeIdentifier(fmt::format("{}_Pipeline{}_State", ctx->GetFunctionPrefix(), id_)),
             [this](CodeGen *codegen) { return codegen_->MakeExpr(GetPipelineStateName()); }),
      driver_(nullptr),
      parallelism_(Parallelism::Parallel),
      check_parallelism_(true),
      nested_(false) {}

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
  auto &state = GetPipelineStateDescriptor();
  return state.DeclareStateEntry(codegen_, name, type_repr);
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

void Pipeline::LinkSourcePipeline(Pipeline *dependency) {
  NOISEPAGE_ASSERT(dependency != nullptr, "Source cannot be null");
  // Add pipeline `dependency` as a nested pipeline
  dependencies_.push_back(dependency);
  // Remove ourselves from the nested pipeline of dependency, if present
  // TODO(Kyle): Is this possible? If so, is this a broken invariant?
  if (std::find(dependency->nested_pipelines_.begin(), dependency->nested_pipelines_.end(), this) !=
      dependency->nested_pipelines_.end()) {
    dependency->nested_pipelines_.erase(
        std::remove(dependency->nested_pipelines_.begin(), dependency->nested_pipelines_.end(), this),
        dependency->nested_pipelines_.end());
  }
}

void Pipeline::LinkNestedPipeline(Pipeline *pipeline, const OperatorTranslator *op) {
  NOISEPAGE_ASSERT(pipeline != nullptr, "Nested pipeline cannot be null");
  // if pipeline is in my dependencies let's not do this to avoid circularity
  if (std::find(dependencies_.begin(), dependencies_.end(), pipeline) == dependencies_.end()) {
    pipeline->nested_pipelines_.push_back(this);
  }
  if (!pipeline->IsNestedPipeline()) {
    pipeline->MarkNested();
    // add to pipeline params
    std::size_t i = 0;
    for (auto &col : op->GetPlan().GetOutputSchema()->GetColumns()) {
      NOISEPAGE_ASSERT(pipeline->extra_pipeline_params_.empty(),
                       "We do not support two pipelines nesting the same pipeline yet");
      pipeline->extra_pipeline_params_.push_back(
          codegen_->MakeField(codegen_->MakeIdentifier("row" + std::to_string(i++)),
                              codegen_->PointerType(codegen_->TplType(sql::GetTypeId(col.GetType())))));
    }
  }
}

void Pipeline::CollectDependencies(std::vector<Pipeline *> *deps) {
  for (auto *pipeline : dependencies_) {
    pipeline->CollectDependencies(deps);
  }

  deps->push_back(this);

  for (auto *pipeline : nested_pipelines_) {
    pipeline->CollectDependencies(deps);
  }
  deps->push_back(this);
}

void Pipeline::CollectDependencies(std::vector<const Pipeline *> *deps) const {
  for (auto *pipeline : dependencies_) {
    pipeline->CollectDependencies(deps);
  }

  for (auto *pipeline : nested_pipelines_) {
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
  // If this pipeline is nested, it doesn't own its pipeline state
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
    std::string result{};
    bool first = true;
    for (auto iter = Begin(), end = End(); iter != end; ++iter) {
      if (!first) {
        result += " --> ";
      }
      first = false;
      std::string plan_type = planner::PlanNodeTypeToString((*iter)->GetPlan().GetPlanNodeType());
      std::transform(plan_type.begin(), plan_type.end(), plan_type.begin(), ::tolower);
      result.append(plan_type);
    }
    EXECUTION_LOG_TRACE("Pipeline-{}: parallel={}, vectorized={}, steps=[{}]", id_, IsParallel(), IsVectorized(),
                        result);
  }

  prepared_ = true;
}

/* ----------------------------------------------------------------------------
  Pipeline Generation: Top-Level
----------------------------------------------------------------------------- */

void Pipeline::GeneratePipeline(ExecutableQueryFragmentBuilder *builder) const {
  NOISEPAGE_ASSERT(!(IsNestedPipeline() && HasOutputCallback()),
                   "Single pipeline cannot both be nested and have an output callback");

  // Declare the pipeline state.
  builder->DeclareStruct(state_.GetType());
  // Generate pipeline state initialization and tear-down functions.
  builder->DeclareFunction(GenerateInitPipelineStateFunction());
  builder->DeclareFunction(GenerateTearDownPipelineStateFunction());

  auto teardown = GenerateTearDownPipelineFunction();

  // Declare top-level functions
  builder->DeclareFunction(GenerateInitPipelineFunction());
  builder->DeclareFunction(GeneratePipelineWorkFunction());
  builder->DeclareFunction(GenerateRunPipelineFunction());
  builder->DeclareFunction(teardown);

  if (HasOutputCallback()) {
    auto run_all = GeneratePipelineRunAllOutputCallbackFunction();
    builder->DeclareFunction(run_all);
    builder->RegisterStep(run_all);
  } else if (!IsNestedPipeline()) {
    // Register the main init, run, tear-down functions as steps, in that order.
    builder->RegisterStep(GenerateInitPipelineFunction());
    builder->RegisterStep(GenerateRunPipelineFunction());
    builder->RegisterStep(teardown);
  }

  builder->AddTeardownFn(teardown);
}

/* ----------------------------------------------------------------------------
  Pipeline Generation: State Setup + Teardown
----------------------------------------------------------------------------- */

ast::FunctionDecl *Pipeline::GenerateInitPipelineStateFunction() const {
  auto name = GetInitPipelineStateFunctionName();
  FunctionBuilder builder{codegen_, name, GetInitPipelineStateParams(), codegen_->Nil()};
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope{codegen_};
    for (auto *op : steps_) {
      op->InitializePipelineState(*this, &builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineStateFunction() const {
  auto name = GetTearDownPipelineStateFunctionName();
  FunctionBuilder builder{codegen_, name, GetTeardownPipelineStateParams(), codegen_->Nil()};
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope{codegen_};
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

/* ----------------------------------------------------------------------------
  Pipeline Generation: RunAll
----------------------------------------------------------------------------- */

ast::FunctionDecl *Pipeline::GeneratePipelineRunAllNestedFunction() const {
  NOISEPAGE_ASSERT(IsNestedPipeline(), "Should only generate a RunAllNested function in nested pipeline");

  const ast::Identifier name = GetRunAllNestedPipelineFunctionName();
  FunctionBuilder builder{codegen_, name, GetRunAllNestedPipelineParams(), codegen_->Nil()};
  {
    CodeGen::CodeScope code_scope{codegen_};

    ast::Identifier pipeline_state = codegen_->MakeFreshIdentifier("pipeline_state");
    builder.Append(codegen_->DeclareVarNoInit(pipeline_state, state_.GetType()->TypeRepr()));
    auto pipeline_state_ptr = codegen_->AddressOf(pipeline_state);

    NOISEPAGE_ASSERT(builder.GetParameterCount() == 1, "Unexpected parameter count for RunAllNested function");
    auto *query_state_param = builder.GetParameterByPosition(0);

    builder.Append(codegen_->Call(GetInitPipelineFunctionName(), {query_state_param, pipeline_state_ptr}));
    builder.Append(codegen_->Call(GetRunPipelineFunctionName(), {query_state_param, pipeline_state_ptr}));
    builder.Append(codegen_->Call(GetTeardownPipelineFunctionName(), {query_state_param, pipeline_state_ptr}));
  }

  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineRunAllOutputCallbackFunction() const {
  NOISEPAGE_ASSERT(HasOutputCallback(),
                   "Should only generate RunAllOutputCallback function for pipeline with output callback");

  const ast::Identifier name = GetRunAllOutputCallbackPipelineFunctionName();
  FunctionBuilder builder{codegen_, name, GetRunAllOutputCallbackPipelineParams(), codegen_->Nil()};

  {
    CodeGen::CodeScope code_scope{codegen_};

    ast::Identifier pipeline_state_id = codegen_->MakeFreshIdentifier("pipelineState");
    builder.Append(codegen_->DeclareVarNoInit(pipeline_state_id, state_.GetType()->TypeRepr()));

    NOISEPAGE_ASSERT(builder.GetParameterCount() == 2, "Unexpected parameter count for RunAllOutputCallback function");
    auto *query_state = builder.GetParameterByPosition(0);
    auto *pipeline_state = codegen_->AddressOf(pipeline_state_id);
    auto *callback = builder.GetParameterByPosition(1);

    builder.Append(codegen_->Call(GetInitPipelineFunctionName(), {query_state, pipeline_state}));
    builder.Append(codegen_->Call(GetRunPipelineFunctionName(), {query_state, pipeline_state, callback}));
    builder.Append(codegen_->Call(GetTeardownPipelineFunctionName(), {query_state, pipeline_state}));
  }

  return builder.Finish();
}

/* ----------------------------------------------------------------------------
  Pipeline Generation: Steps
----------------------------------------------------------------------------- */

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction() const {
  auto query_state = compilation_context_->GetQueryState();
  const ast::Identifier name = GetInitPipelineFunctionName();

  auto parameters = GetInitPipelineParams();
  FunctionBuilder builder{codegen_, name, std::move(parameters), codegen_->Nil()};
  {
    CodeGen::CodeScope code_scope{codegen_};
    ast::Expr *state_ptr = query_state->GetStatePointer(codegen_);

    if (IsNestedPipeline() || HasOutputCallback()) {
      // No TLS reset in nested pipelines
      // NOTE: Assumes the pipeline state is always the final parameter
      const auto pipeline_state_index = builder.GetParameterCount() - 1;
      auto *pipeline_state = builder.GetParameterByPosition(pipeline_state_index);
      builder.Append(codegen_->Call(GetInitPipelineStateFunctionName(), {state_ptr, pipeline_state}));
    } else {
      auto &state = GetPipelineStateDescriptor();
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      ast::Identifier tls = codegen_->MakeFreshIdentifier("threadStateContainer");
      // var tls = @execCtxGetTLS(exec_ctx)
      builder.Append(codegen_->DeclareVarWithInit(tls, codegen_->ExecCtxGetTLS(exec_ctx)));
      // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
      builder.Append(codegen_->TLSReset(codegen_->MakeExpr(tls), state.GetTypeName(),
                                        GetInitPipelineStateFunctionName(), GetTearDownPipelineStateFunctionName(),
                                        state_ptr));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction() const {
  bool started_tracker = false;
  const ast::Identifier name = GetRunPipelineFunctionName();
  FunctionBuilder builder{codegen_, name, GetRunPipelineParams(), codegen_->Nil()};
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope{codegen_};

    // TODO(abalakum): This shouldn't actually be dependent on order and the loop can be simplified
    // after issue #1154 is fixed
    // Let the operators perform some initialization work in this pipeline.
    for (auto iter = Begin(), end = End(); iter != end; ++iter) {
      (*iter)->BeginPipelineWork(*this, &builder);
    }

    // TODO(Kyle): I think this is wrong for nested pipelines / output callbacks
    // var pipelineState = @tlsGetCurrentThreadState(...)
    auto exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
    auto tls = codegen_->ExecCtxGetTLS(exec_ctx);
    auto state_type = GetPipelineStateDescriptor().GetTypeName();
    auto state = codegen_->TLSAccessCurrentThreadState(tls, state_type);
    builder.Append(codegen_->DeclareVarWithInit(GetPipelineStateName(), state));

    // Launch pipeline work.
    if (IsParallel()) {
      driver_->LaunchWork(&builder, GetPipelineWorkFunctionName());
    } else {
      // Serial pipeline
      InjectStartResourceTracker(&builder, false);
      started_tracker = true;

      std::vector<ast::Expr *> params{codegen_->MakeExpr(GetQueryStateName()),
                                      codegen_->MakeExpr(GetPipelineStateName())};
      if (IsNestedPipeline()) {
        const auto run_params = builder.GetParameters();
        auto begin = run_params.cbegin();
        std::advance(begin, params.size());
        params.insert(params.end(), begin, run_params.cend());
      }

      if (HasOutputCallback()) {
        params.push_back(codegen_->MakeExpr(GetOutputCallback()->GetName()));
      }

      builder.Append(codegen_->Call(GetPipelineWorkFunctionName(), std::move(params)));
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

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction() const {
  FunctionBuilder builder{codegen_, GetPipelineWorkFunctionName(), GetPipelineWorkParams(), codegen_->Nil()};
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope{codegen_};
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

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction() const {
  const ast::Identifier name = GetTeardownPipelineFunctionName();
  FunctionBuilder builder{codegen_, name, GetTeardownPipelineParams(), codegen_->Nil()};
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope{codegen_};
    if (IsNestedPipeline() || HasOutputCallback()) {
      // NOTE: Assumes pipeline state is always final parameter to call
      const auto pipeline_state_index = builder.GetParameterCount() - 1;
      auto query_state = compilation_context_->GetQueryState()->GetStatePointer(codegen_);
      auto pipeline_state = builder.GetParameterByPosition(pipeline_state_index);
      auto call = codegen_->Call(GetTearDownPipelineStateFunctionName(), {query_state, pipeline_state});
      builder.Append(codegen_->MakeStmt(call));
    } else {
      // Tear down thread local state if parallel pipeline.
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      builder.Append(codegen_->TLSClear(codegen_->ExecCtxGetTLS(exec_ctx)));
      auto call = codegen_->CallBuiltin(ast::Builtin::EnsureTrackersStopped, {exec_ctx});
      builder.Append(codegen_->MakeStmt(call));
    }
  }
  return builder.Finish();
}

/* ----------------------------------------------------------------------------
  Pipeline Function Parameter Definition
----------------------------------------------------------------------------- */

util::RegionVector<ast::FieldDecl *> Pipeline::GetInitPipelineStateParams() const {
  // fun QueryX_PipelineY_InitPipelineState(queryState, pipelineState)
  return PipelineParams();
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetTeardownPipelineStateParams() const {
  // fun QueryX_PipelineY_TeardownPipelineState(queryState, pipelineState)
  return PipelineParams();
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetRunAllNestedPipelineParams() const {
  NOISEPAGE_ASSERT(IsNestedPipeline(), "RunAllNested should only be generated for nested pipelines");
  // fun QueryX_PipelineY_RunAll(queryState)
  return QueryParams();
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetRunAllOutputCallbackPipelineParams() const {
  NOISEPAGE_ASSERT(HasOutputCallback(),
                   "RunAllOutputCallback should only be generated for pipeline with output callback");
  // fun QueryX_PipelineY_RunAll(queryState, udfLambda)
  util::RegionVector<ast::FieldDecl *> params{QueryParams()};
  params.push_back(codegen_->MakeField(
      GetOutputCallback()->GetName(), codegen_->LambdaType(GetOutputCallback()->GetFunctionLiteralExpr()->TypeRepr())));
  return params;
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetInitPipelineParams() const {
  /**
   * Common Case:
   *  fun QueryX_PipelineY_Init(queryState)
   *
   * Nested Pipeline:
   *  fun QueryX_PipelineY_InitPipeline(queryState, pipelineState)
   *
   * Output Callback:
   *  fun QueryX_PipelineY_InitPipeline(queryState, pipelineState)
   */

  util::RegionVector<ast::FieldDecl *> params{QueryParams()};
  if (IsNestedPipeline() || HasOutputCallback()) {
    const auto &state = GetPipelineStateDescriptor();
    ast::FieldDecl *pipeline_state_ptr =
        codegen_->MakeField(codegen_->MakeFreshIdentifier("pipeline_state"),
                            codegen_->PointerType(codegen_->MakeExpr(state.GetTypeName())));
    params.push_back(pipeline_state_ptr);
  }
  return params;
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetRunPipelineParams() const {
  /**
   * Common Case:
   *  fun QueryX_PipelineY_Run(queryState)
   *
   * Nested Pipeline:
   *  fun QueryX_PipelineY_Run(queryState, pipelineState)
   *
   * Output Callback:
   *  fun QueryX_PipelineY_Run(queryState, outputCallback)
   */

  util::RegionVector<ast::FieldDecl *> params{QueryParams()};

  if (IsNestedPipeline() || HasOutputCallback()) {
    params.push_back(codegen_->MakeField(GetPipelineStateName(), codegen_->PointerType(state_.GetTypeName())));
  }

  for (auto *field : extra_pipeline_params_) {
    params.push_back(field);
  }

  if (HasOutputCallback()) {
    params.push_back(
        codegen_->MakeField(GetOutputCallback()->GetName(),
                            codegen_->LambdaType(GetOutputCallback()->GetFunctionLiteralExpr()->TypeRepr())));
  }

  return params;
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetPipelineWorkParams() const {
  util::RegionVector<ast::FieldDecl *> params{PipelineParams()};
  for (auto *field : extra_pipeline_params_) {
    params.push_back(field);
  }

  if (IsParallel()) {
    auto additional_params = driver_->GetWorkerParams();
    params.insert(params.end(), additional_params.cbegin(), additional_params.cend());
  }

  if (HasOutputCallback()) {
    params.push_back(
        codegen_->MakeField(GetOutputCallback()->GetName(),
                            codegen_->LambdaType(GetOutputCallback()->GetFunctionLiteralExpr()->TypeRepr())));
  }

  return params;
}

util::RegionVector<ast::FieldDecl *> Pipeline::GetTeardownPipelineParams() const {
  /**
   * Common Case:
   *  QueryX_PipelineY_Teardown(queryState)
   *
   * Nested Pipeline:
   *  QueryX_PipelineY_Teardown(queryState, pipelineState)
   *
   * Output Callback:
   *  QueryX_PipelineY_Teardown(queryState, pipelineState)
   */
  util::RegionVector<ast::FieldDecl *> params{QueryParams()};
  if (IsNestedPipeline() || HasOutputCallback()) {
    ast::FieldDecl *pipeline_state =
        codegen_->MakeField(codegen_->MakeFreshIdentifier("pipeline_state"),
                            codegen_->PointerType(codegen_->MakeExpr(GetPipelineStateDescriptor().GetTypeName())));
    params.push_back(pipeline_state);
  }
  return params;
}

util::RegionVector<ast::FieldDecl *> Pipeline::QueryParams() const { return compilation_context_->QueryParams(); }

util::RegionVector<ast::FieldDecl *> Pipeline::PipelineParams() const {
  // The main query parameters
  util::RegionVector<ast::FieldDecl *> pipeline_params{QueryParams()};
  // Tag on the pipeline state
  auto &state = GetPipelineStateDescriptor();
  ast::Expr *pipeline_state = codegen_->PointerType(codegen_->MakeExpr(state.GetTypeName()));
  pipeline_params.push_back(codegen_->MakeField(GetPipelineStateName(), pipeline_state));
  return pipeline_params;
}

/* ----------------------------------------------------------------------------
  Pipeline Calls
----------------------------------------------------------------------------- */

std::vector<ast::Expr *> Pipeline::CallSingleRunPipelineFunction() const {
  NOISEPAGE_ASSERT(!IsNestedPipeline(), "Can't call a nested pipeline like this");
  return {
      codegen_->Call(GetInitPipelineFunctionName(), {compilation_context_->GetQueryState()->GetStatePointer(codegen_)}),
      codegen_->Call(GetRunPipelineFunctionName(), {compilation_context_->GetQueryState()->GetStatePointer(codegen_)}),
      codegen_->Call(GetTeardownPipelineFunctionName(),
                     {compilation_context_->GetQueryState()->GetStatePointer(codegen_)})};
}

void Pipeline::CallNestedRunPipelineFunction(WorkContext *ctx, const OperatorTranslator *op,
                                             FunctionBuilder *function) const {
  std::vector<ast::Expr *> stmts;
  auto p_state = codegen_->MakeFreshIdentifier("nested_state");
  auto p_state_ptr = codegen_->AddressOf(p_state);

  std::vector<ast::Expr *> params_vec{compilation_context_->GetQueryState()->GetStatePointer(codegen_)};
  params_vec.push_back(p_state_ptr);

  for (std::size_t i = 0; i < op->GetPlan().GetOutputSchema()->GetColumns().size(); i++) {
    params_vec.push_back(codegen_->AddressOf(op->GetOutput(ctx, i)));
  }

  function->Append(codegen_->DeclareVarNoInit(p_state, codegen_->MakeExpr(GetPipelineStateDescriptor().GetTypeName())));
  function->Append(codegen_->Call(GetInitPipelineFunctionName(),
                                  {compilation_context_->GetQueryState()->GetStatePointer(codegen_), p_state_ptr}));
  function->Append(codegen_->Call(GetRunPipelineFunctionName(), params_vec));
  function->Append(codegen_->Call(GetTeardownPipelineFunctionName(),
                                  {compilation_context_->GetQueryState()->GetStatePointer(codegen_), p_state_ptr}));
}

std::vector<ast::Expr *> Pipeline::CallRunPipelineFunction() const {
  std::vector<ast::Expr *> calls;
  std::vector<const Pipeline *> pipelines;
  CollectDependencies(&pipelines);
  for (auto pipeline : pipelines) {
    if (!pipeline->IsNestedPipeline() || (pipeline == this)) {
      for (auto call : CallSingleRunPipelineFunction()) {
        calls.push_back(call);
      }
    }
  }
  return calls;
}

/* ----------------------------------------------------------------------------
  Variable + Function Identifiers
----------------------------------------------------------------------------- */

ast::Identifier Pipeline::GetQueryStateName() const { return compilation_context_->GetQueryStateName(); }

ast::Identifier Pipeline::GetPipelineStateName() const { return codegen_->MakeIdentifier("pipelineState"); }

ast::Identifier Pipeline::GetInitPipelineStateFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("InitPipelineState"));
}

ast::Identifier Pipeline::GetTearDownPipelineStateFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDownPipelineState"));
}

ast::Identifier Pipeline::GetRunAllNestedPipelineFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("RunAllNested"));
}

ast::Identifier Pipeline::GetRunAllOutputCallbackPipelineFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("RunAllOutputCallback"));
}

ast::Identifier Pipeline::GetInitPipelineFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("Init"));
}

ast::Identifier Pipeline::GetTeardownPipelineFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDown"));
}

ast::Identifier Pipeline::GetRunPipelineFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName("Run"));
}

ast::Identifier Pipeline::GetPipelineWorkFunctionName() const {
  return codegen_->MakeIdentifier(CreatePipelineFunctionName(IsParallel() ? "ParallelWork" : "SerialWork"));
}

std::string Pipeline::CreatePipelineFunctionName(const std::string &func_name) const {
  auto result = fmt::format("{}_Pipeline{}", compilation_context_->GetFunctionPrefix(), id_);
  if (!func_name.empty()) {
    result += "_" + func_name;
  }
  return result;
}

/* ----------------------------------------------------------------------------
  Additional Helpers
----------------------------------------------------------------------------- */

ast::Expr *Pipeline::GetNestedInputArg(const std::size_t index) const {
  NOISEPAGE_ASSERT(IsNestedPipeline(), "Requested nested input argument on non-nested pipeline");
  NOISEPAGE_ASSERT(index < extra_pipeline_params_.size(), "Requested nested index argument out of range");
  return codegen_->UnaryOp(parsing::Token::Type::STAR, codegen_->MakeExpr(extra_pipeline_params_[index]->Name()));
}

}  // namespace noisepage::execution::compiler
