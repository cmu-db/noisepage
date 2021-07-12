#include "execution/exec/execution_context.h"

#include "common/error/error_code.h"
#include "common/thread_context.h"
#include "execution/sql/value.h"
#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"
#include "replication/primary_replication_manager.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/modeling/operating_unit_util.h"
#include "storage/recovery/recovery_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::execution::exec {

OutputBuffer *ExecutionContext::OutputBufferNew() {
  if (schema_ == nullptr) {
    return nullptr;
  }

  // Use C++ placement new
  auto size = sizeof(OutputBuffer);
  auto *buffer = reinterpret_cast<OutputBuffer *>(mem_pool_->Allocate(size));
  new (buffer) OutputBuffer(mem_pool_.get(), schema_->GetColumns().size(), ComputeTupleSize(schema_), callback_);
  return buffer;
}

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

uint64_t ExecutionContext::ReplicationGetLastTransactionId() const {
  // Both ReplicationManager and RecoveryManager participate in replication, which defines the last transaction ID
  // sent from the primary and applied on the replicas respectively.
  if (replication_manager_ == DISABLED || recovery_manager_ == DISABLED) {
    throw EXECUTION_EXCEPTION("Replication is disabled. There is no record ID to get.",
                              common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  // On the primary, this builtin returns the last transaction that was sent by the ReplicationManager.
  if (replication_manager_->IsPrimary()) {
    return replication_manager_->GetAsPrimary()->GetLastSentTransactionId().UnderlyingValue();
  }
  // On a replica, this builtin returns the last transaction was was applied on the RecoveryManager.
  NOISEPAGE_ASSERT(replication_manager_->IsReplica(), "Neither primary nor replica?");
  return recovery_manager_->GetLastAppliedTransactionId().UnderlyingValue();
}

void ExecutionContext::RegisterThreadWithMetricsManager() {
  if (noisepage::common::thread_context.metrics_store_ == nullptr && GetMetricsManager()) {
    GetMetricsManager()->RegisterThread();
  }
}

void ExecutionContext::EnsureTrackersStopped() {
  // Resource trackers are not automatically terminated at the end of query execution. If an
  // exception is thrown during execution between StartPipelineTracker and EndPipelineTracker,
  // then the trackers will keep on running (assuming the ThreadContext stays alive).
  //
  // If a transaction has aborted through \@abortTxn, then it is very probable that EndPipelineTracker
  // was not called to stop the resource tracker. This check here terminates the resource trackers
  // if they are still running (with the caveat that no metrics will be recorded).
  if (GetTxn()->MustAbort() && noisepage::common::thread_context.resource_tracker_.IsRunning()) {
    noisepage::common::thread_context.resource_tracker_.Stop();
  }

  // Codegen is responsible for guaranteeing that StartPipelineTrackers and EndPipelineTrackers
  // are properly matched (if a thread calls StartPipelineTracker, it must call EndPipelineTracker
  // prior to the ThreadContext getting destroyed). In the case query execution completes normally
  // without any exceptional control flow, the following checks that the trackers are fully stopped.
  if (noisepage::common::thread_context.metrics_store_ != nullptr &&
      noisepage::common::thread_context.resource_tracker_.IsRunning()) {
    UNREACHABLE("Resource Trackers should have stopped");
  }
}

void ExecutionContext::AggregateMetricsThread() {
  if (GetMetricsManager()) {
    GetMetricsManager()->Aggregate();
  }
}

void ExecutionContext::StartResourceTracker(metrics::MetricsComponent component) {
  NOISEPAGE_ASSERT(component == metrics::MetricsComponent::EXECUTION,
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
    if (common::thread_context.resource_tracker_.IsRunning()) {
      common::thread_context.nesting_depth_++;
    } else {
      // Start the resource tracker.
      NOISEPAGE_ASSERT(!common::thread_context.resource_tracker_.IsRunning(), "ResourceTrackers cannot be nested");
      common::thread_context.resource_tracker_.Start();
      mem_tracker_->Reset();
    }

    NOISEPAGE_ASSERT(pipeline_operating_units_ != nullptr, "PipelineOperatingUnits should not be null");
  }
}

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id,
                                          selfdriving::ExecOUFeatureVector *ouvec) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    if (common::thread_context.nesting_depth_ == 0) {
      common::thread_context.resource_tracker_.Stop();
      auto mem_size = mem_tracker_->GetAllocatedSize();
      if (memory_use_override_) {
        mem_size = memory_use_override_value_;
      }

      common::thread_context.resource_tracker_.SetMemory(mem_size);
    } else {
      common::thread_context.nesting_depth_--;
    }
    const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

    NOISEPAGE_ASSERT(pipeline_id == ouvec->pipeline_id_, "Incorrect feature vector pipeline id?");
    selfdriving::ExecutionOperatingUnitFeatureVector features(ouvec->pipeline_features_->begin(),
                                                              ouvec->pipeline_features_->end());
    common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline_id, execution_mode_,
                                                              std::move(features), resource_metrics);
  }
}

void ExecutionContext::InitializeOUFeatureVector(selfdriving::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id) {
  auto *vec = new (ouvec) selfdriving::ExecOUFeatureVector();
  vec->pipeline_id_ = pipeline_id;

  auto &features = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  vec->pipeline_features_ = std::make_unique<execution::sql::MemPoolVector<selfdriving::ExecutionOperatingUnitFeature>>(
      features.begin(), features.end(), GetMemoryPool());

  // Update num_concurrent
  for (auto &feature : *vec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

void ExecutionContext::InitializeParallelOUFeatureVector(selfdriving::ExecOUFeatureVector *ouvec,
                                                         pipeline_id_t pipeline_id) {
  auto *vec = new (ouvec) selfdriving::ExecOUFeatureVector();
  vec->pipeline_id_ = pipeline_id;
  vec->pipeline_features_ =
      std::make_unique<execution::sql::MemPoolVector<selfdriving::ExecutionOperatingUnitFeature>>(GetMemoryPool());

  bool found_blocking = false;
  selfdriving::ExecutionOperatingUnitFeature feature;
  auto features = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  for (auto &feat : features) {
    if (selfdriving::OperatingUnitUtil::IsOperatingUnitTypeBlocking(feat.GetExecutionOperatingUnitType())) {
      NOISEPAGE_ASSERT(!found_blocking, "Pipeline should only have 1 blocking");
      found_blocking = true;
      feature = feat;
    }
  }

  if (!found_blocking) {
    NOISEPAGE_ASSERT(false, "Pipeline should have 1 blocking");
    return;
  }

  switch (feature.GetExecutionOperatingUnitType()) {
    case selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD:
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN, feature);
      break;
    case selfdriving::ExecutionOperatingUnitType::AGGREGATE_BUILD:
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_MERGE_AGGBUILD, feature);
      break;
    case selfdriving::ExecutionOperatingUnitType::SORT_BUILD:
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_SORT_STEP, feature);
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP, feature);
      break;
    case selfdriving::ExecutionOperatingUnitType::SORT_TOPK_BUILD:
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_STEP, feature);
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::PARALLEL_SORT_TOPK_MERGE_STEP,
                                            feature);
      break;
    case selfdriving::ExecutionOperatingUnitType::CREATE_INDEX_MAIN:
      // Just copy the feature that has been setup already.
      vec->pipeline_features_->emplace_back(selfdriving::ExecutionOperatingUnitType::CREATE_INDEX_MAIN, feature);
      break;
    default:
      NOISEPAGE_ASSERT(false, "Unsupported parallel OU");
  }

  // Update num_concurrent
  for (auto &feature : *vec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

void ExecutionContext::RegisterHook(size_t hook_idx, HookFn hook) {
  NOISEPAGE_ASSERT(hook_idx < hooks_.capacity(), "Incorrect number of reserved hooks");
  hooks_[hook_idx] = hook;
}

void ExecutionContext::InvokeHook(size_t hook_index, void *tls, void *arg) {
  if (hook_index < hooks_.size() && hooks_[hook_index] != nullptr) {
    hooks_[hook_index](this->query_state_, tls, arg);
  }
}

void ExecutionContext::InitHooks(size_t num_hooks) { hooks_.resize(num_hooks); }

}  // namespace noisepage::execution::exec
