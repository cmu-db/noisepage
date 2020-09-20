#include "execution/exec/execution_context.h"

#include "brain/operating_unit.h"
#include "brain/operating_unit_util.h"
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

void ExecutionContext::RegisterThread() {
  if (terrier::common::thread_context.metrics_store_ == nullptr && GetMetricsManager()) {
    GetMetricsManager()->RegisterThread();
  }
}

void ExecutionContext::CheckTrackersStopped() {
  if (terrier::common::thread_context.metrics_store_ != nullptr &&
      terrier::common::thread_context.resource_tracker_.IsRunning()) {
    UNREACHABLE("Resource Trackers should have stopped");
  }
}

void ExecutionContext::AggregateMetricsThread() {
  if (GetMetricsManager()) {
    GetMetricsManager()->Aggregate();
  }
}

void ExecutionContext::StartResourceTracker(metrics::MetricsComponent component) {
  TERRIER_ASSERT(component == metrics::MetricsComponent::EXECUTION,
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
    const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordExecutionData(name, len, execution_mode_, resource_metrics);
  }
}

void ExecutionContext::StartPipelineTracker(pipeline_id_t pipeline_id) {
  constexpr metrics::MetricsComponent component = metrics::MetricsComponent::EXECUTION_PIPELINE;

  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(component)) {
    // Start the resource tracker.
    TERRIER_ASSERT(!common::thread_context.resource_tracker_.IsRunning(), "ResourceTrackers cannot be nested");
    common::thread_context.resource_tracker_.Start();
    mem_tracker_->Reset();

    TERRIER_ASSERT(pipeline_operating_units_ != nullptr, "PipelineOperatingUnits should not be null");
  }
}

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id,
                                          brain::ExecOUFeatureVector *ouvec) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    common::thread_context.resource_tracker_.Stop();
    auto mem_size = mem_tracker_->GetAllocatedSize();
    if (memory_use_override_) {
      mem_size = memory_use_override_value_;
    }

    common::thread_context.resource_tracker_.SetMemory(mem_size);
    const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

    TERRIER_ASSERT(pipeline_id == ouvec->pipeline_id_, "Incorrect feature vector pipeline id?");
    common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline_id, execution_mode_,
                                                              std::move(*ouvec->pipeline_features_), resource_metrics);
  }
}

void ExecutionContext::InitializeExecOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id) {
  if (ouvec->pipeline_features_ != nullptr) {
    ouvec->Destroy();
  }

  ouvec->pipeline_id_ = pipeline_id;
  ouvec->pipeline_features_ =
      new brain::ExecutionOperatingUnitFeatureVector(pipeline_operating_units_->GetPipelineFeatures(pipeline_id));

  // Update num_concurrent
  for (auto &feature : *ouvec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

void ExecutionContext::InitializeParallelOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id) {
  ouvec->pipeline_id_ = pipeline_id;

  bool found_blocking = false;
  brain::ExecutionOperatingUnitFeature feature;
  auto features = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  for (auto &feat : features) {
    if (brain::OperatingUnitUtil::IsOperatingUnitTypeBlocking(feat.GetExecutionOperatingUnitType())) {
      TERRIER_ASSERT(!found_blocking, "Pipeline should only have 1 blocking");
      found_blocking = true;
      feature = feat;
    }
  }

  if (!found_blocking) {
    TERRIER_ASSERT(false, "Pipeline should have 1 blocking");
    return;
  }

  ouvec->pipeline_features_ = new brain::ExecutionOperatingUnitFeatureVector();
  switch (feature.GetExecutionOperatingUnitType()) {
    case brain::ExecutionOperatingUnitType::HASHJOIN_BUILD:
      ouvec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN, feature);
      break;
    case brain::ExecutionOperatingUnitType::AGGREGATE_BUILD:
      ouvec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_MERGE_AGGBUILD, feature);
      break;
    case brain::ExecutionOperatingUnitType::SORT_BUILD:
      ouvec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP, feature);
      ouvec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP, feature);
      break;
    case brain::ExecutionOperatingUnitType::CREATE_INDEX:
      ouvec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::CREATE_INDEX_MAIN, feature);
      break;
    default:
      TERRIER_ASSERT(false, "Unsupported parallel OU");
  }

  // Update num_concurrent
  for (auto &feature : *ouvec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

}  // namespace terrier::execution::exec
