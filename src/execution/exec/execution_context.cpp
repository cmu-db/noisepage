#include "execution/exec/execution_context.h"

#include "brain/operating_unit.h"
#include "common/thread_context.h"
#include "execution/sql/value.h"
#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::exec {

uint32_t ExecutionContext::ComputeTupleSize(const planner::OutputSchema *schema) {
  uint32_t tuple_size = 0;
  for (const auto &col : schema->GetColumns()) {
    auto alignment = sql::ValUtil::GetSqlAlignment(col.GetType());
    if (!common::MathUtil::IsAligned(tuple_size, alignment)) {
      tuple_size = static_cast<uint32_t>(common::MathUtil::AlignTo(tuple_size, alignment));
    }
    tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
  }
  return tuple_size;
}

void ExecutionContext::StartResourceTracker(metrics::MetricsComponent component) {
  TERRIER_ASSERT(
      component == metrics::MetricsComponent::EXECUTION || component == metrics::MetricsComponent::EXECUTION_PIPELINE,
      "StartResourceTracker() invoked with incorrect MetricsComponent");

  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(component)) {
    // start the operating unit resource tracker
    common::thread_context.resource_tracker_.Start();
    mem_tracker_->Reset();
  }
}

void ExecutionContext::EndResourceTracker(const char *name, uint32_t len) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    common::thread_context.resource_tracker_.Stop();
    common::thread_context.resource_tracker_.SetMemory(mem_tracker_->GetAllocatedSize());
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordExecutionData(name, len, execution_mode_, resource_metrics);
  }
}

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    common::thread_context.resource_tracker_.Stop();
    auto mem_size = mem_tracker_->GetAllocatedSize();
    if (memory_use_override_) {
      mem_size = memory_use_override_value_;
    }

    common::thread_context.resource_tracker_.SetMemory(mem_size);
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

    // TODO(wz2): With a query cache, see if we can avoid this copy
    TERRIER_ASSERT(pipeline_operating_units_ != nullptr, "PipelineOperatingUnits should not be null");
    brain::ExecutionOperatingUnitFeatureVector features(pipeline_operating_units_->GetPipelineFeatures(pipeline));
    common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline, execution_mode_, std::move(features),
                                                              resource_metrics);
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

}  // namespace terrier::execution::exec
