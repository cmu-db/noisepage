#include "brain/operating_unit.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/value.h"

namespace terrier::execution::exec {

char *ExecutionContext::StringAllocator::Allocate(std::size_t size) {
  if (tracker_ != nullptr) tracker_->Increment(size);
  return reinterpret_cast<char *>(region_.Allocate(size));
}

uint32_t ExecutionContext::ComputeTupleSize(const planner::OutputSchema *schema) {
  uint32_t tuple_size = 0;
  for (const auto &col : schema->GetColumns()) {
    tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
  }
  return tuple_size;
}

void ExecutionContext::StartResourceTracker() {
  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::EXECUTION)) {
    // start the operating unit resource tracker
    common::thread_context.resource_tracker_.Start();
    mem_tracker_->Reset();
  }
}

void ExecutionContext::EndResourceTracker(const char *name, uint32_t len) {
  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::EXECUTION)) {
    common::thread_context.resource_tracker_.Stop();
    common::thread_context.resource_tracker_.SetMemory(mem_tracker_->GetAllocatedSize());
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordExecutionData(name, len, execution_mode_, resource_metrics);
  }
}

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline) {
  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::EXECUTION)) {
    common::thread_context.resource_tracker_.Stop();
    common::thread_context.resource_tracker_.SetMemory(mem_tracker_->GetAllocatedSize());
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

    // TODO(wz2): With a query cahce, see if we can avoid this copy
    brain::OperatingUnitFeatureVector features(operating_units_->GetPipelineFeatures(pipeline));
    common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline, execution_mode_, std::move(features), resource_metrics);
  }
}

}  // namespace terrier::execution::exec
