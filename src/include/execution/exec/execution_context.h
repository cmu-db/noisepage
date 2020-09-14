#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "common/managed_pointer.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/sql/memory_tracker.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/region.h"
#include "metrics/metrics_defs.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::brain {
class PipelineOperatingUnits;
}  // namespace terrier::brain

namespace terrier::catalog {
class CatalogAccessor;
}  // namespace terrier::catalog

namespace terrier::execution::exec {

/**
 * Execution Context: Stores information handed in by upper layers.
 * TODO(Amadou): This class will change once we know exactly what we get from upper layers.
 */
class EXPORT ExecutionContext {
 public:
  /**
   * Constructor
   * @param db_oid oid of the database
   * @param txn transaction used by this query
   * @param callback callback function for outputting
   * @param schema the schema of the output
   * @param accessor the catalog accessor of this query
   * @param exec_settings The execution settings to run with.
   */
  ExecutionContext(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                   const OutputCallback &callback, const planner::OutputSchema *schema,
                   const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                   const exec::ExecutionSettings &exec_settings)
      : exec_settings_(exec_settings),
        db_oid_(db_oid),
        txn_(txn),
        mem_tracker_(std::make_unique<sql::MemoryTracker>()),
        mem_pool_(std::make_unique<sql::MemoryPool>(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_))),
        buffer_(schema == nullptr ? nullptr
                                  : std::make_unique<OutputBuffer>(mem_pool_.get(), schema->GetColumns().size(),
                                                                   ComputeTupleSize(schema), callback)),
        thread_state_container_(std::make_unique<sql::ThreadStateContainer>(mem_pool_.get())),
        accessor_(accessor) {}

  /**
   * @return the transaction used by this query
   */
  common::ManagedPointer<transaction::TransactionContext> GetTxn() { return txn_; }

  /**
   * @return the output buffer used by this query
   */
  OutputBuffer *GetOutputBuffer() { return buffer_.get(); }

  /**
   * @return The thread state container.
   */
  sql::ThreadStateContainer *GetThreadStateContainer() { return thread_state_container_.get(); }

  /**
   * @return the memory pool
   */
  sql::MemoryPool *GetMemoryPool() { return mem_pool_.get(); }

  /**
   * @return the string allocator
   */
  sql::VarlenHeap *GetStringAllocator() { return &string_allocator_; }

  /**
   * @param schema the schema of the output
   * @return the size of tuple with this final_schema
   */
  static uint32_t ComputeTupleSize(const planner::OutputSchema *schema);

  /**
   * @return The catalog accessor.
   */
  catalog::CatalogAccessor *GetAccessor() { return accessor_.Get(); }

  /** @return The execution settings. */
  const exec::ExecutionSettings &GetExecutionSettings() const { return exec_settings_; }

  /**
   * Start the resource tracker
   */
  void StartResourceTracker(metrics::MetricsComponent component);

  /**
   * End the resource tracker and record the metrics
   * @param name the string name get printed out with the time
   * @param len the length of the string name
   */
  void EndResourceTracker(const char *name, uint32_t len);

  /**
   * Start the resource tracker for a pipeline.
   * @param pipeline_id id of the pipeline
   */
  void StartPipelineTracker(pipeline_id_t pipeline_id);

  /**
   * End the resource tracker for a pipeline and record the metrics
   * @param query_id query identifier
   * @param pipeline_id id of the pipeline
   */
  void EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id);

  /**
   * Get the specified feature.
   * @param value The destination for the value of the feature's attribute.
   * @param pipeline_id The ID of the pipeline whose feature is to be recorded.
   * @param feature_id The ID of the feature to be recorded.
   * @param feature_attribute The attribute of the feature to record.
   */
  void GetFeature(uint32_t *value, pipeline_id_t pipeline_id, feature_id_t feature_id,
                  brain::ExecutionOperatingUnitFeatureAttribute feature_attribute);

  /**
   * Record the specified feature.
   * @param pipeline_id The ID of the pipeline whose feature is to be recorded.
   * @param feature_id The ID of the feature to be recorded.
   * @param feature_attribute The attribute of the feature to record.
   * @param value The value for the feature's attribute.
   */
  void RecordFeature(pipeline_id_t pipeline_id, feature_id_t feature_id,
                     brain::ExecutionOperatingUnitFeatureAttribute feature_attribute, uint32_t value);

  /**
   * @return the db oid
   */
  catalog::db_oid_t DBOid() { return db_oid_; }

  /**
   * Set the mode for this execution.
   * This only records the mode and serves the metrics collection purpose, which does not have any impact on the
   * actual execution.
   * @param mode the integer value of the execution mode to record
   */
  void SetExecutionMode(uint8_t mode) { execution_mode_ = mode; }

  /**
   * Set the accessor
   * @param accessor The catalog accessor.
   */
  void SetAccessor(const common::ManagedPointer<catalog::CatalogAccessor> accessor) { accessor_ = accessor; }

  /**
   * Set the execution parameters.
   * @param params The execution parameters.
   */
  void SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> params) {
    params_ = params;
  }

  /**
   * @param param_idx index of parameter to access
   * @return immutable parameter at provided index
   */
  const parser::ConstantValueExpression &GetParam(uint32_t param_idx) const;

  /**
   * INSERT, UPDATE, and DELETE queries return a number for the rows affected, so this should be incremented in the root
   * nodes of the query
   */
  uint64_t &RowsAffected() { return rows_affected_; }

  /**
   * Set the PipelineOperatingUnits
   * @param op PipelineOperatingUnits for executing the given query
   */
  void SetPipelineOperatingUnits(common::ManagedPointer<brain::PipelineOperatingUnits> op) {
    pipeline_operating_units_ = op;
  }

  /** Increment or decrement the number of rows affected. */
  void AddRowsAffected(int64_t num_rows) { rows_affected_ += num_rows; }

  /**
   * Overrides recording from memory tracker
   * @param memory_use Correct memory value to record
   */
  void SetMemoryUseOverride(uint32_t memory_use) {
    memory_use_override_ = true;
    memory_use_override_value_ = memory_use;
  }

 private:
  const exec::ExecutionSettings &exec_settings_;
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unique_ptr<sql::MemoryTracker> mem_tracker_;
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  std::unique_ptr<OutputBuffer> buffer_;
  // Container for thread-local state.
  // During parallel processing, execution threads access their thread-local state from this container.
  std::unique_ptr<sql::ThreadStateContainer> thread_state_container_;
  // TODO(WAN): EXEC PORT we used to push the memory tracker into the string allocator, do this
  sql::VarlenHeap string_allocator_;
  common::ManagedPointer<brain::PipelineOperatingUnits> pipeline_operating_units_;

  pipeline_id_t current_pipeline_features_id_;
  std::vector<brain::ExecutionOperatingUnitFeature> current_pipeline_features_;

  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> params_;
  uint8_t execution_mode_;
  uint64_t rows_affected_ = 0;

  bool memory_use_override_ = false;
  uint32_t memory_use_override_value_ = 0;
};
}  // namespace terrier::execution::exec
