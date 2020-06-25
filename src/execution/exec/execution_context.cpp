#include "execution/exec/execution_context.h"

#include "brain/operating_unit.h"
#include "common/thread_context.h"
#include "execution/sql/value.h"
#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::exec {

ExecutionContext::ExecutionContext(catalog::db_oid_t db_oid,
                                   common::ManagedPointer<transaction::TransactionContext> txn,
                                   const OutputCallback &callback, const planner::OutputSchema *schema,
                                   const common::ManagedPointer<catalog::CatalogAccessor> accessor)
    : db_oid_(db_oid),
      txn_(txn),
      mem_tracker_(std::make_unique<sql::MemoryTracker>()),
      mem_pool_(std::make_unique<sql::MemoryPool>(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_))),
      buffer_(schema == nullptr ? nullptr
                                : std::make_unique<OutputBuffer>(mem_pool_.get(), schema->GetColumns().size(),
                                                                 ComputeTupleSize(schema), callback)),
      string_allocator_(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_)),
      accessor_(accessor) {}

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
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::EXECUTION_PIPELINE)) {
    common::thread_context.resource_tracker_.Stop();
    common::thread_context.resource_tracker_.SetMemory(mem_tracker_->GetAllocatedSize());
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
