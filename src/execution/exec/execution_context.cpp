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
    common::thread_context.resource_tracker_.Start();
    mem_tracker_->Reset();
    // Save a copy of the pipeline's features as the features will be updated in-place later.
    TERRIER_ASSERT(pipeline_operating_units_ != nullptr, "PipelineOperatingUnits should not be null");
    current_pipeline_features_id_ = pipeline_id;
    current_pipeline_features_ = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  }
}

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    common::thread_context.resource_tracker_.Stop();
    auto mem_size = mem_tracker_->GetAllocatedSize();
    if (memory_use_override_) {
      mem_size = memory_use_override_value_;
    }

    common::thread_context.resource_tracker_.SetMemory(mem_size);
    const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

    common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline_id, execution_mode_,
                                                              std::move(current_pipeline_features_), resource_metrics);
  }
}

void ExecutionContext::GetFeature(uint32_t *value, pipeline_id_t pipeline_id, feature_id_t feature_id,
                                  brain::ExecutionOperatingUnitFeatureAttribute feature_attribute) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    TERRIER_ASSERT(pipeline_id == current_pipeline_features_id_, "That's not the current pipeline.");
    auto &features = current_pipeline_features_;
    for (auto &feature : features) {
      if (feature_id == feature.GetFeatureId()) {
        uint64_t val;
        switch (feature_attribute) {
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
            val = feature.GetNumRows();
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
            val = feature.GetCardinality();
            break;
          }
          default:
            UNREACHABLE("Invalid feature attribute.");
        }
        *value = val;
        break;
      }
    }
  }
}

void ExecutionContext::RecordFeature(pipeline_id_t pipeline_id, feature_id_t feature_id,
                                     brain::ExecutionOperatingUnitFeatureAttribute feature_attribute, uint32_t value) {
  constexpr metrics::MetricsComponent component = metrics::MetricsComponent::EXECUTION_PIPELINE;

  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentEnabled(component)) {
    TERRIER_ASSERT(pipeline_id == current_pipeline_features_id_, "That's not the current pipeline.");

    UNUSED_ATTRIBUTE bool recorded = false;
    auto &features = current_pipeline_features_;
    for (auto &feature : features) {
      if (feature_id == feature.GetFeatureId()) {
        switch (feature_attribute) {
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
            feature.SetNumRows(value);
            recorded = true;
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
            feature.SetCardinality(value);
            recorded = true;
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_LOOPS: {
            feature.SetNumLoops(value);
            recorded = true;
            break;
          }
          default:
            UNREACHABLE("Invalid feature attribute.");
        }
        break;
      }
    }
    TERRIER_ASSERT(recorded, "Nothing was recorded. OperatingUnitRecorder hacks are probably necessary.");
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

}  // namespace terrier::execution::exec
