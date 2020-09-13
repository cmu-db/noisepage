#include "execution/compiler/operator/operator_translator.h"

#include "brain/operating_unit_util.h"
#include "common/error/exception.h"
#include "common/json.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::compiler {

std::atomic<execution::translator_id_t> OperatorTranslator::translator_id_counter{20000};  // arbitrary number

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline, brain::ExecutionOperatingUnitType feature_type)
    : translator_id_(translator_id_counter++),
      plan_(plan),
      compilation_context_(compilation_context),
      pipeline_(pipeline),
      feature_type_(feature_type) {
  TERRIER_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
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
  TERRIER_ASSERT(child_translator != nullptr, "Missing translator for child!");
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

StateDescriptor::Entry OperatorTranslator::CounterDeclare(const std::string &counter_name) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // Declare a new counter in the query state.
    ast::Expr *counter_type = codegen->BuiltinType(ast::BuiltinType::Uint32);
    return compilation_context_->GetQueryState()->DeclareStateEntry(codegen, counter_name, counter_type);
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

void OperatorTranslator::FeatureRecord(FunctionBuilder *function, brain::ExecutionOperatingUnitType feature_type,
                                       brain::ExecutionOperatingUnitFeatureAttribute attrib, const Pipeline &pipeline,
                                       ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    const auto &feature = brain::OperatingUnitUtil::GetFeature(GetTranslatorId(), features, feature_type);
    ast::Expr *record =
        codegen->ExecCtxRecordFeature(GetExecutionContext(), pipeline_id, feature.GetFeatureId(), attrib, val);
    function->Append(record);
  }
}

void OperatorTranslator::FeatureArithmeticRecordSet(FunctionBuilder *function, const Pipeline &pipeline,
                                                    execution::translator_id_t translator_id, ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    for (const auto &feature : features) {
      bool is_same_translator = feature.GetTranslatorId() == translator_id;
      bool is_arith = feature.GetExecutionOperatingUnitType() > brain::ExecutionOperatingUnitType::PLAN_OPS_DELIMITER;
      if (is_same_translator && is_arith) {
        for (const auto &attrib : {brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS,
                                   brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY}) {
          ast::Expr *record =
              codegen->ExecCtxRecordFeature(GetExecutionContext(), pipeline_id, feature.GetFeatureId(), attrib, val);
          function->Append(record);
        }
      }
    }
  }
}

void OperatorTranslator::FeatureArithmeticRecordMul(FunctionBuilder *function, const Pipeline &pipeline,
                                                    execution::translator_id_t translator_id, ast::Expr *val) const {
  auto *codegen = GetCodeGen();

  if (IsCountersEnabled()) {
    // @execCtxRecordFeature(execCtx, pipeline_id, feature_id, feature_attribute, val * old_feature_val)
    const pipeline_id_t pipeline_id = pipeline.GetPipelineId();
    const auto &features = codegen->GetPipelineOperatingUnits()->GetPipelineFeatures(pipeline_id);
    for (const auto &feature : features) {
      bool is_same_translator = feature.GetTranslatorId() == translator_id;
      bool is_arith = feature.GetExecutionOperatingUnitType() > brain::ExecutionOperatingUnitType::PLAN_OPS_DELIMITER;
      if (is_same_translator && is_arith) {
        for (const auto &attrib : {brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS,
                                   brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY}) {
          ast::Expr *mul = codegen->BinaryOp(
              parsing::Token::Type::STAR, val,
              codegen->CallBuiltin(ast::Builtin::ExecutionContextGetFeature,
                                   {GetExecutionContext(), codegen->Const32(pipeline.GetPipelineId().UnderlyingValue()),
                                    codegen->Const32(feature.GetFeatureId().UnderlyingValue()),
                                    codegen->Const32(static_cast<uint8_t>(attrib))}));
          ast::Expr *record =
              codegen->ExecCtxRecordFeature(GetExecutionContext(), pipeline_id, feature.GetFeatureId(), attrib, mul);
          function->Append(record);
        }
      }
    }
  }
}

}  // namespace terrier::execution::compiler
