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
      state_var_(codegen_->MakeIdentifier("pipelineState")),
      state_(codegen_->MakeIdentifier(fmt::format("P{}{}_State", ctx->GetFunctionPrefix(), id_)),
             [this](CodeGen *codegen) { return codegen_->MakeExpr(state_var_); }),
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
  auto &state = GetPipelineStateDescriptor();
  ast::Expr *pipeline_state = codegen_->PointerType(codegen_->MakeExpr(state.GetTypeName()));
  query_params.push_back(codegen_->MakeField(state_var_, pipeline_state));
  return query_params;
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
  if (!pipeline->nested_) {
    pipeline->nested_ = true;
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

ast::FunctionDecl *Pipeline::GeneratePipelineWrapperFunction(ast::LambdaExpr *output_callback) const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("RunAll"));
  auto params = compilation_context_->QueryParams();
  auto run_params = params;
  if (output_callback != nullptr) {
    run_params.push_back(codegen_->MakeField(
        output_callback->GetName(), codegen_->LambdaType(output_callback->GetFunctionLiteralExpr()->TypeRepr())));
  }
  FunctionBuilder builder(codegen_, name, std::move(run_params), codegen_->Nil());
  {
    CodeGen::CodeScope code_scope(codegen_);
    ast::Identifier p_state = codegen_->MakeFreshIdentifier("pipeline_state");
    builder.Append(codegen_->DeclareVarNoInit(p_state, state_.GetType()->TypeRepr()));
    auto query_state_param = builder.GetParameterByPosition(0);
    auto p_state_ptr = codegen_->AddressOf(p_state);
    auto lambda_call = builder.GetParameterByPosition(1);
    builder.Append(codegen_->Call(GetInitPipelineFunctionName(), {query_state_param, p_state_ptr}));
    builder.Append(codegen_->Call(GetRunPipelineFunctionName(), {query_state_param, p_state_ptr, lambda_call}));
    builder.Append(codegen_->Call(GetTeardownPipelineFunctionName(), {query_state_param, p_state_ptr}));
  }

  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GenerateInitPipelineFunction(ast::LambdaExpr *output_callback) const {
  auto query_state = compilation_context_->GetQueryState();
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Init"));
  auto params = compilation_context_->QueryParams();
  ast::FieldDecl *p_state_ptr = nullptr;
  auto &state = GetPipelineStateDescriptor();
  uint32_t p_state_ind = 0;
  if (nested_ || output_callback != nullptr) {
    p_state_ptr = codegen_->MakeField(codegen_->MakeFreshIdentifier("pipeline_state"),
                                      codegen_->PointerType(codegen_->MakeExpr(state.GetTypeName())));
    params.push_back(p_state_ptr);
    p_state_ind = params.size() - 1;
  }
  FunctionBuilder builder(codegen_, name, std::move(params), codegen_->Nil());
  {
    CodeGen::CodeScope code_scope(codegen_);
    // var tls = @execCtxGetTLS(exec_ctx)
    ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
    ast::Identifier tls = codegen_->MakeFreshIdentifier("threadStateContainer");
    builder.Append(codegen_->DeclareVarWithInit(tls, codegen_->ExecCtxGetTLS(exec_ctx)));

    // @tlsReset(tls, @sizeOf(ThreadState), init, tearDown, queryState)
    ast::Expr *state_ptr = query_state->GetStatePointer(codegen_);
    if (!nested_ && output_callback == nullptr) {
      builder.Append(codegen_->TLSReset(codegen_->MakeExpr(tls), state.GetTypeName(),
                                        GetSetupPipelineStateFunctionName(), GetTearDownPipelineStateFunctionName(),
                                        state_ptr));
    } else {
      // no TLS reset if pipeline is nested
      auto pipeline_state = builder.GetParameterByPosition(p_state_ind);
      builder.Append(codegen_->Call(GetSetupPipelineStateFunctionName(), {state_ptr, pipeline_state}));
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *Pipeline::GeneratePipelineWorkFunction(ast::LambdaExpr *output_callback) const {
  auto params = PipelineParams();
  for (auto field : extra_pipeline_params_) {
    params.push_back(field);
  }

  if (IsParallel()) {
    auto additional_params = driver_->GetWorkerParams();
    params.insert(params.end(), additional_params.begin(), additional_params.end());
  }

  if (output_callback != nullptr) {
    params.push_back(codegen_->MakeField(output_callback->GetName(),
                                         codegen_->LambdaType(output_callback->GetFunctionLiteralExpr()->TypeRepr())));
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

std::vector<ast::Expr *> Pipeline::CallSingleRunPipelineFunction() const {
  NOISEPAGE_ASSERT(!nested_, "can't call a nested pipeline like this");
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

  std::vector<ast::Expr *> params_vec = {compilation_context_->GetQueryState()->GetStatePointer(codegen_)};
  params_vec.push_back(p_state_ptr);

  for (size_t i = 0; i < op->GetPlan().GetOutputSchema()->GetColumns().size(); i++) {
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
    if (!pipeline->nested_ || (pipeline == this)) {
      for (auto call : CallSingleRunPipelineFunction()) {
        calls.push_back(call);
      }
    }
  }
  return calls;
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

ast::Expr *Pipeline::GetNestedInputArg(const std::size_t index) const {
  NOISEPAGE_ASSERT(nested_, "Requested nested input argument on non-nested pipeline");
  NOISEPAGE_ASSERT(index < extra_pipeline_params_.size(), "Requested nested index argument out of range");
  return codegen_->UnaryOp(parsing::Token::Type::STAR, codegen_->MakeExpr(extra_pipeline_params_[index]->Name()));
}

ast::FunctionDecl *Pipeline::GenerateRunPipelineFunction(query_id_t query_id, ast::LambdaExpr *output_callback) const {
  bool started_tracker = false;
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("Run"));
  auto params = compilation_context_->QueryParams();
  if (nested_ || output_callback != nullptr) {
    params.push_back(codegen_->MakeField(state_var_, codegen_->PointerType(state_.GetTypeName())));
  }
  for (auto field : extra_pipeline_params_) {
    params.push_back(field);
  }
  if (output_callback != nullptr) {
    params.push_back(codegen_->MakeField(output_callback->GetName(),
                                         codegen_->LambdaType(output_callback->GetFunctionLiteralExpr()->TypeRepr())));
  }
  FunctionBuilder builder(codegen_, name, std::move(params), codegen_->Nil());
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
    auto state_type = GetPipelineStateDescriptor().GetTypeName();
    auto state = codegen_->TLSAccessCurrentThreadState(tls, state_type);
    builder.Append(codegen_->DeclareVarWithInit(state_var_, state));

    // Launch pipeline work.
    if (IsParallel()) {
      driver_->LaunchWork(&builder, GetWorkFunctionName());
    } else {
      // SerialWork(queryState, pipelineState)
      //      if(!nested_) {
      InjectStartResourceTracker(&builder, false);
      started_tracker = true;

      std::vector<ast::Expr *> args{builder.GetParameterByPosition(0), codegen_->MakeExpr(state_var_)};
      if (nested_) {
        size_t i = args.size();
        ast::Expr *arg = builder.GetParameterByPosition(i++);
        while (arg != nullptr) {
          args.push_back(arg);
          arg = builder.GetParameterByPosition(i++);
        }
      }
      if (output_callback != nullptr && !nested_) {
        args.push_back(codegen_->MakeExpr(output_callback->GetName()));
      }
      builder.Append(codegen_->Call(GetWorkFunctionName(), args));
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

ast::FunctionDecl *Pipeline::GenerateTearDownPipelineFunction(ast::LambdaExpr *output_callback) const {
  auto name = codegen_->MakeIdentifier(CreatePipelineFunctionName("TearDown"));
  auto params = compilation_context_->QueryParams();
  ast::FieldDecl *p_state_ptr = nullptr;
  auto &state = GetPipelineStateDescriptor();
  uint32_t p_state_index = 0;
  if (nested_ || output_callback != nullptr) {
    p_state_ptr = codegen_->MakeField(codegen_->MakeFreshIdentifier("pipeline_state"),
                                      codegen_->PointerType(codegen_->MakeExpr(state.GetTypeName())));
    params.push_back(p_state_ptr);
    p_state_index = params.size() - 1;
  }

  FunctionBuilder builder(codegen_, name, std::move(params), codegen_->Nil());
  {
    // Begin a new code scope for fresh variables.
    CodeGen::CodeScope code_scope(codegen_);
    if (!nested_ && output_callback == nullptr) {
      // Tear down thread local state if parallel pipeline.
      ast::Expr *exec_ctx = compilation_context_->GetExecutionContextPtrFromQueryState();
      builder.Append(codegen_->TLSClear(codegen_->ExecCtxGetTLS(exec_ctx)));

      auto call = codegen_->CallBuiltin(ast::Builtin::EnsureTrackersStopped, {exec_ctx});
      builder.Append(codegen_->MakeStmt(call));
    } else {
      auto query_state = compilation_context_->GetQueryState();
      auto state_ptr = query_state->GetStatePointer(codegen_);

      auto pipeline_state = builder.GetParameterByPosition(p_state_index);
      auto call = codegen_->Call(GetTearDownPipelineStateFunctionName(), {state_ptr, pipeline_state});
      builder.Append(codegen_->MakeStmt(call));
    }
  }
  return builder.Finish();
}

void Pipeline::GeneratePipeline(ExecutableQueryFragmentBuilder *builder, query_id_t query_id,
                                ast::LambdaExpr *output_callback) const {
  // Declare the pipeline state.
  builder->DeclareStruct(state_.GetType());
  // Generate pipeline state initialization and tear-down functions.
  builder->DeclareFunction(GenerateSetupPipelineStateFunction());
  builder->DeclareFunction(GenerateTearDownPipelineStateFunction());

  // Generate main pipeline logic.
  builder->DeclareFunction(GeneratePipelineWorkFunction(output_callback));
  builder->DeclareFunction(GenerateRunPipelineFunction(query_id, output_callback));
  builder->DeclareFunction(GenerateInitPipelineFunction(output_callback));
  auto teardown = GenerateTearDownPipelineFunction(output_callback);
  builder->DeclareFunction(teardown);

  // Register the main init, run, tear-down functions as steps, in that order.
  if (output_callback != nullptr) {
    auto fn = GeneratePipelineWrapperFunction(output_callback);
    builder->DeclareFunction(fn);
    builder->RegisterStep(fn);
  } else if (!nested_) {
    builder->RegisterStep(GenerateInitPipelineFunction(output_callback));
    builder->RegisterStep(GenerateRunPipelineFunction(query_id, output_callback));
    builder->RegisterStep(teardown);
  }
  builder->AddTeardownFn(teardown);
}

}  // namespace noisepage::execution::compiler
