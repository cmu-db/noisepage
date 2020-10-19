#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "common/managed_pointer.h"
#include "execution/exec/execution_settings.h"
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

namespace terrier::metrics {
class MetricsManager;
}  // namespace terrier::metrics

namespace terrier::execution::exec {

/**
 * Execution Context: Stores information handed in by upper layers.
 * TODO(Amadou): This class will change once we know exactly what we get from upper layers.
 */
class EXPORT ExecutionContext {
 public:
  /**
   * Hook Function
   *
   * Hook functions stemmed from a discussion with Prashanth over the best way
   * to gather runtime metrics for C++ code. The solution is "hooks". A hook is a
   * TPL function generated for a pipeline by a specific translator that contains the
   * necessary instrumentation logic.
   *
   * Hooks are registered prior to a function call that may require use of a hook
   * and then unregistered afterwards. Note that the typical process for adding
   * hooks to instrument some C++ code (that is invoked from TPL) is as follows:
   * 1. Decide what C++ call from TPL requires instrumenting (this is function F)
   * 2. Determine the number of hook sites required inside function F
   * 3. Modify the translator(s) that invoke F to generate hook functions
   * 4. Determine a mapping from hook sites in F to codegen'ed hook functions
   * 4. Prior to codegen-ing the call to F, codegen calls to ExecutionContextRegisterHook
   *    that map each hook function to a particular call site
   * 5. After codegen-ing the call to F, codegen a call to ExecutionContextClearHooks
   * 6. Modify F to invoke the designated hook at each call site
   *
   * Convention: First argument is the query state.
   *             second argument is the thread state.
   *             Third is opaque function argument.
   */
  using HookFn = void (*)(void *, void *, void *);

  /**
   * Constructor
   * @param db_oid oid of the database
   * @param txn transaction used by this query
   * @param callback callback function for outputting
   * @param schema the schema of the output
   * @param accessor the catalog accessor of this query
   * @param exec_settings The execution settings to run with.
   * @param metrics_manager The metrics manager for recording metrics
   */
  ExecutionContext(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                   const OutputCallback &callback, const planner::OutputSchema *schema,
                   const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                   const exec::ExecutionSettings &exec_settings,
                   common::ManagedPointer<metrics::MetricsManager> metrics_manager)
      : exec_settings_(exec_settings),
        db_oid_(db_oid),
        txn_(txn),
        mem_tracker_(std::make_unique<sql::MemoryTracker>()),
        mem_pool_(std::make_unique<sql::MemoryPool>(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_))),
        schema_(schema),
        callback_(callback),
        thread_state_container_(std::make_unique<sql::ThreadStateContainer>(mem_pool_.get())),
        accessor_(accessor),
        metrics_manager_(metrics_manager) {}

  /**
   * @return the transaction used by this query
   */
  common::ManagedPointer<transaction::TransactionContext> GetTxn() { return txn_; }

  /**
   * Constructs a new Output Buffer for outputting query results to consumers
   * @return newly created output buffer
   */
  OutputBuffer *OutputBufferNew();

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
   * @param ouvec OU feature vector for the pipeline
   */
  void EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id, brain::ExecOUFeatureVector *ouvec);

  /**
   * Initializes an OU feature vector for a given pipeline
   * @param ouvec OU Feature Vector to initialize
   * @param pipeline_id Pipeline to initialize with
   */
  void InitializeOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id);

  /**
   * Initializes an OU feature vector for a given parallel step (i.e hashjoin_build, sort_build, agg_build)
   * @param ouvec OU Feature Vector to initialize
   * @param pipeline_id Pipeline to initialize with
   */
  void InitializeParallelOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id);

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
  uint32_t &RowsAffected() { return rows_affected_; }

  /**
   * Set the PipelineOperatingUnits
   * @param op PipelineOperatingUnits for executing the given query
   */
  void SetPipelineOperatingUnits(common::ManagedPointer<brain::PipelineOperatingUnits> op) {
    pipeline_operating_units_ = op;
  }

  /**
   * @return PipelineOperatingUnits
   */
  common::ManagedPointer<brain::PipelineOperatingUnits> GetPipelineOperatingUnits() {
    return pipeline_operating_units_;
  }

  /** Increment or decrement the number of rows affected. */
  void AddRowsAffected(int64_t num_rows) { rows_affected_ += num_rows; }

  /**
   * If the calling thread is not registered with any metrics manager, this function
   * will register the calling thread with the metrics manager held by this ExecutionContext.
   *
   * This is particularly useful during parallel query execution: assume that thread A
   * constructs an ExecutionContext with metrics manager B. Then all threads [T] spawned by TBB
   * during parallel query execution that invoke this function will be registered with
   * metrics manager B.
   */
  void RegisterThreadWithMetricsManager();

  /**
   * Requests that the metrics manager attached to this execution context
   * aggregate metrics from registered threads.
   */
  void AggregateMetricsThread();

  /**
   * Checks that the trackers for the current thread are stopped
   */
  void CheckTrackersStopped();

  /**
   * @return metrics manager used by execution context
   */
  common::ManagedPointer<metrics::MetricsManager> GetMetricsManager() { return metrics_manager_; }

  /**
   * @return query identifier
   */
  execution::query_id_t GetQueryId() { return query_id_; }

  /**
   * Set the current executing query identifier
   */
  void SetQueryId(execution::query_id_t query_id) { query_id_ = query_id; }

  /**
   * Overrides recording from memory tracker
   * This should never be used by parallel threads directly
   * @param memory_use Correct memory value to record
   */
  void SetMemoryUseOverride(uint32_t memory_use) {
    memory_use_override_ = true;
    memory_use_override_value_ = memory_use;
  }

  /**
   * Sets the opaque query state pointer for the current query invocation
   * @param query_state QueryState
   */
  void SetQueryState(void *query_state) { query_state_ = query_state; }

  /**
   * Sets the estimated concurrency of a parallel operation.
   * This value is used when initializing an ExecOUFeatureVector
   *
   * @note this value is reset by setting it to 0.
   * @param estimate Estimated number of concurrent tasks
   */
  void SetNumConcurrentEstimate(uint32_t estimate) { num_concurrent_estimate_ = estimate; }

  /**
   * Invoke a hook function if a hook function is available
   * @param hook_index Index of hook function to invoke
   * @param tls TLS argument
   * @param arg Opaque argument to pass
   */
  void InvokeHook(size_t hook_index, void *tls, void *arg);

  /**
   * Registers a hook function
   * @param hook_idx Hook index to register function
   * @param hook Function to register
   */
  void RegisterHook(size_t hook_idx, HookFn hook);

  /**
   * Initializes hooks_ to a certain capacity
   * @param num_hooks Number of hooks needed
   */
  void InitHooks(size_t num_hooks);

  /**
   * Clears hooks_
   */
  void ClearHooks() { hooks_.clear(); }

 private:
  query_id_t query_id_{execution::query_id_t(0)};
  exec::ExecutionSettings exec_settings_;
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unique_ptr<sql::MemoryTracker> mem_tracker_;
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  std::unique_ptr<OutputBuffer> buffer_ = nullptr;
  const planner::OutputSchema *schema_ = nullptr;
  const OutputCallback &callback_;
  // Container for thread-local state.
  // During parallel processing, execution threads access their thread-local state from this container.
  std::unique_ptr<sql::ThreadStateContainer> thread_state_container_;
  // TODO(WAN): EXEC PORT we used to push the memory tracker into the string allocator, do this
  sql::VarlenHeap string_allocator_;
  common::ManagedPointer<brain::PipelineOperatingUnits> pipeline_operating_units_{nullptr};

  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> params_;
  uint8_t execution_mode_;
  uint32_t rows_affected_ = 0;

  bool memory_use_override_ = false;
  uint32_t memory_use_override_value_ = 0;
  uint32_t num_concurrent_estimate_ = 0;
  std::vector<HookFn> hooks_{};
  void *query_state_;
};
}  // namespace terrier::execution::exec
