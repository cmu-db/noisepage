#include "execution/compiler/operator/operator_translator.h"

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/json.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "self_driving/modeling/operating_unit_util.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

std::atomic<execution::translator_id_t> OperatorTranslator::translator_id_counter{20000};  // arbitrary number

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline, selfdriving::ExecutionOperatingUnitType feature_type)
    : translator_id_(translator_id_counter++),
      plan_(plan),
      compilation_context_(compilation_context),
      pipeline_(pipeline),
      feature_type_(feature_type) {
  NOISEPAGE_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
  // Register this operator.
  pipeline->RegisterStep(this);
  // Prepare all output expressions.
  for (const auto &output_column : plan.GetOutputSchema()->GetColumns()) {
    compilation_context->Prepare(*output_column.GetExpr());
  }
}

ast::Expr *OperatorTranslator::GetOutput(WorkContext *context, uint32_t attr_idx) const {
  // Check valid output column.
  const auto output_schema = plan_.GetOutputSchema();
  if (attr_idx >= output_schema->NumColumns()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Codegen: Cannot read column {} from '{}' with output schema {}", attr_idx,
                    planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), output_schema->ToJson().dump()),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  const auto output_expression = output_schema->GetColumn(attr_idx).GetExpr();
  return context->DeriveValue(*output_expression, this);
}

ast::Expr *OperatorTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // Check valid child.
  if (child_idx >= plan_.GetChildrenSize()) {
    throw EXECUTION_EXCEPTION(fmt::format("Codegen: Plan type '{}' does not have child at index {}",
                                          planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), child_idx),
                              common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  // Check valid output column from child.
  auto child_translator = compilation_context_->LookupTranslator(*plan_.GetChild(child_idx));
  NOISEPAGE_ASSERT(child_translator != nullptr, "Missing translator for child!");
  return child_translator->GetOutput(context, attr_idx);
}

CodeGen *OperatorTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

ast::Expr *OperatorTranslator::GetQueryStatePtr() const {
  return compilation_context_->GetQueryState()->GetStatePointer(GetCodeGen());
}

ast::Expr *OperatorTranslator::GetExecutionContext() const {
  return compilation_context_->GetExecutionContextPtrFromQueryState();
}

ast::Expr *OperatorTranslator::GetThreadStateContainer() const {
  return GetCodeGen()->ExecCtxGetTLS(GetExecutionContext());
}

ast::Expr *OperatorTranslator::GetMemoryPool() const {
  return GetCodeGen()->ExecCtxGetMemoryPool(GetExecutionContext());
}

void OperatorTranslator::GetAllChildOutputFields(const uint32_t child_index, const std::string &field_name_prefix,
                                                 util::RegionVector<ast::FieldDecl *> *fields) const {
  auto *codegen = GetCodeGen();

  // Reserve now to reduce allocations.
  const auto child_output_schema = plan_.GetChild(child_index)->GetOutputSchema();
  fields->reserve(child_output_schema->NumColumns());

  // Add columns to output.
  uint32_t attr_idx = 0;
  for (const auto &col : plan_.GetChild(child_index)->GetOutputSchema()->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(field_name_prefix + std::to_string(attr_idx++));
    auto type = codegen->TplType(sql::GetTypeId(col.GetExpr()->GetReturnValueType()));
    fields->emplace_back(codegen->MakeField(field_name, type));
  }
}

bool OperatorTranslator::IsCountersEnabled() const { return compilation_context_->IsCountersEnabled(); }

bool OperatorTranslator::IsPipelineMetricsEnabled() const { return compilation_context_->IsPipelineMetricsEnabled(); }

StateDescriptor::Entry OperatorTranslator::CounterDeclare(const std::string &counter_name, Pipeline *pipeline) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // Declare a new counter in the pipeline state
    ast::Expr *counter_type = codegen->BuiltinType(ast::BuiltinType::Uint32);
    return pipeline->DeclarePipelineStateEntry(counter_name, counter_type);
  }

  return StateDescriptor::Entry();
}

void OperatorTranslator::CounterSet(FunctionBuilder *function, const StateDescriptor::Entry &counter,
                                    int64_t val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // counter = val
    ast::Stmt *set = codegen->Assign(counter.Get(codegen), codegen->Const32(val));
    function->Append(set);
  }
}

void OperatorTranslator::CounterSetExpr(FunctionBuilder *function, const StateDescriptor::Entry &counter,
                                        ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // counter = val
    ast::Stmt *set = codegen->Assign(counter.Get(codegen), val);
    function->Append(set);
  }
}

void OperatorTranslator::CounterAdd(FunctionBuilder *function, const StateDescriptor::Entry &counter,
                                    int64_t val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // counter = counter + val
    ast::Expr *plus = codegen->BinaryOp(parsing::Token::Type::PLUS, counter.Get(codegen), codegen->Const32(val));
    ast::Stmt *increment = codegen->Assign(counter.Get(codegen), plus);
    function->Append(increment);
  }
}

void OperatorTranslator::CounterAdd(FunctionBuilder *function, const StateDescriptor::Entry &counter,
                                    ast::Identifier val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // counter = counter + val
    ast::Expr *plus = codegen->BinaryOp(parsing::Token::Type::PLUS, counter.Get(codegen), codegen->MakeExpr(val));
    ast::Stmt *increment = codegen->Assign(counter.Get(codegen), plus);
    function->Append(increment);
  }
}

ast::Expr *OperatorTranslator::CounterVal(StateDescriptor::Entry entry) const {
  if (IsCountersEnabled()) {
    return entry.Get(GetCodeGen());
  }
  return nullptr;
}

void OperatorTranslator::FeatureRecord(FunctionBuilder *function, selfdriving::ExecutionOperatingUnitType feature_type,
                                       selfdriving::ExecutionOperatingUnitFeatureAttribute attrib,
                                       const Pipeline &pipeline, ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled() && IsPipelineMetricsEnabled()) {
    auto non_parallel_type = selfdriving::OperatingUnitUtil::GetNonParallelType(feature_type);
    bool is_parallel = non_parallel_type != selfdriving::ExecutionOperatingUnitType::INVALID;
    feature_type = (is_parallel) ? non_parallel_type : feature_type;

    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    const auto &feature = selfdriving::OperatingUnitUtil::GetFeature(GetTranslatorId(), features, feature_type);
    ast::Expr *record = codegen->ExecOUFeatureVectorRecordFeature(
        pipeline.OUFeatureVecPtr(), pipeline_id, feature.GetFeatureId(), attrib,
        selfdriving::ExecutionOperatingUnitFeatureUpdateMode::SET, val);
    function->Append(record);
  }
}

void OperatorTranslator::FeatureArithmeticRecordSet(FunctionBuilder *function, const Pipeline &pipeline,
                                                    execution::translator_id_t translator_id, ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled() && IsPipelineMetricsEnabled()) {
    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    for (const auto &feature : features) {
      bool is_same_translator = feature.GetTranslatorId() == translator_id;
      bool is_arith =
          feature.GetExecutionOperatingUnitType() > selfdriving::ExecutionOperatingUnitType::PLAN_OPS_DELIMITER;
      if (is_same_translator && is_arith) {
        for (const auto &attrib : {selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS,
                                   selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY}) {
          ast::Expr *record = codegen->ExecOUFeatureVectorRecordFeature(
              pipeline.OUFeatureVecPtr(), pipeline_id, feature.GetFeatureId(), attrib,
              selfdriving::ExecutionOperatingUnitFeatureUpdateMode::SET, val);
          function->Append(record);
        }
      }
    }
  }
}

void OperatorTranslator::FeatureArithmeticRecordMul(FunctionBuilder *function, const Pipeline &pipeline,
                                                    execution::translator_id_t translator_id, ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled() && IsPipelineMetricsEnabled()) {
    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val * old_feature_val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    for (const auto &feature : features) {
      bool is_same_translator = feature.GetTranslatorId() == translator_id;
      bool is_arith =
          feature.GetExecutionOperatingUnitType() > selfdriving::ExecutionOperatingUnitType::PLAN_OPS_DELIMITER;
      if (is_same_translator && is_arith) {
        for (const auto &attrib : {selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS,
                                   selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY}) {
          ast::Expr *record = codegen->ExecOUFeatureVectorRecordFeature(
              pipeline.OUFeatureVecPtr(), pipeline_id, feature.GetFeatureId(), attrib,
              selfdriving::ExecutionOperatingUnitFeatureUpdateMode::MULT, val);
          function->Append(record);
        }
      }
    }
  }
}

util::RegionVector<ast::FieldDecl *> OperatorTranslator::GetHookParams(const Pipeline &pipeline, ast::Identifier *arg,
                                                                       ast::Expr *arg_type) const {
  auto *codegen = GetCodeGen();

  auto params = pipeline.PipelineParams();
  if (arg != nullptr) {
    params.push_back(codegen->MakeField(*arg, arg_type));
  } else {
    ast::Identifier dummy = codegen->MakeIdentifier("dummyArg");
    auto *type = codegen->PointerType(codegen->BuiltinType(ast::BuiltinType::Nil));
    params.push_back(codegen->MakeField(dummy, type));
  }

  return params;
}

}  // namespace noisepage::execution::compiler
