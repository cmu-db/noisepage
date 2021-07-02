#pragma once

#include <memory>
#include <optional>
#include <stack>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/sql/memory_tracker.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/value.h"
#include "execution/util/region.h"
#include "execution/vm/execution_mode.h"
#include "metrics/metrics_defs.h"
#include "planner/plannodes/output_schema.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/modeling/operating_unit_defs.h"

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::metrics {
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::replication {
class ReplicationManager;
}  // namespace noisepage::replication

namespace noisepage::selfdriving {
class PipelineOperatingUnits;
}  // namespace noisepage::selfdriving

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::execution::exec {

class ExecutionSettings;

/**
 * The ExecutionContext class stores information handed in by upper layers.
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

  /* --------------------------------------------------------------------------
    Getters / Setters
  -------------------------------------------------------------------------- */

  /** @return The identifier for the associated query */
  execution::query_id_t GetQueryId() { return query_id_; }

  /**
   * Set the current executing query identifier.
   * @param query_id The query identifier
   */
  void SetQueryId(execution::query_id_t query_id) { query_id_ = query_id; }

  /** @return The database OID. */
  catalog::db_oid_t DBOid() { return db_oid_; }

  /** @return The transaction associated with this execution context */
  common::ManagedPointer<transaction::TransactionContext> GetTxn() { return txn_; }

  /** @return The execution mode for the execution context */
  vm::ExecutionMode GetExecutionMode() const { return execution_mode_; }

  /**
   * Set the execution mode for the execution context.
   * @param execution_mode The desired execution mode
   *
   * NOTE: Most of the time one should avoid calling this
   * function directly; the execution mode for the ExecutionContext
   * instance is automatically set in ExecutableQuery::Run() to
   * the execution mode in which the query is executed.
   */
  void SetExecutionMode(const vm::ExecutionMode execution_mode) { execution_mode_ = execution_mode; }

  /** @return The execution settings. */
  const exec::ExecutionSettings &GetExecutionSettings() const { return execution_settings_; }

  /** @return The catalog accessor associated with this execution context */
  catalog::CatalogAccessor *GetAccessor() { return accessor_.Get(); }

  /** @return The metrics manager associated with this execution context */
  common::ManagedPointer<metrics::MetricsManager> GetMetricsManager() { return metrics_manager_; }

  /** @return The memory pool for this execution context */
  sql::MemoryPool *GetMemoryPool() { return mem_pool_.get(); }

  /** @return The thread state container */
  sql::ThreadStateContainer *GetThreadStateContainer() { return thread_state_container_.get(); }

  /** @return The string allocator for this execution context */
  sql::VarlenHeap *GetStringAllocator() { return &string_allocator_; }

  /** @return The pipeline operating units for the execution context */
  common::ManagedPointer<selfdriving::PipelineOperatingUnits> GetPipelineOperatingUnits() {
    return pipeline_operating_units_;
  }

  /**
   * Set the pipeline operating units for the execution context.
   * @param op pipeline operating units for executing the query
   */
  void SetPipelineOperatingUnits(common::ManagedPointer<selfdriving::PipelineOperatingUnits> op) {
    pipeline_operating_units_ = op;
  }

  /** @return The number of rows affected by the current execution, e.g., INSERT/DELETE/UPDATE. */
  uint32_t GetRowsAffected() const { return rows_affected_; }

  /**
   * Increment or decrement the number of rows affected.
   * @param num_rows The delta for the number of rows affected
   */
  void AddRowsAffected(int64_t num_rows) { rows_affected_ += num_rows; }

  /**
   * Overrides recording from memory tracker.
   * NOTE: This should never be used by parallel threads directly
   * @param memory_use Correct memory value to record
   */
  void SetMemoryUseOverride(uint32_t memory_use) {
    memory_use_override_ = true;
    memory_use_override_value_ = memory_use;
  }

  /**
   * Sets the estimated concurrency of a parallel operation.
   * This value is used when initializing an ExecOUFeatureVector
   *
   * @note this value is reset by setting it to 0.
   * @param estimate Estimated number of concurrent tasks
   */
  void SetNumConcurrentEstimate(uint32_t estimate) { num_concurrent_estimate_ = estimate; }

  /**
   * Sets the opaque query state pointer for the current query invocation.
   * @param query_state QueryState
   */
  void SetQueryState(void *query_state) { query_state_ = query_state; }

  /* --------------------------------------------------------------------------
    Resource Metrics Collection
  -------------------------------------------------------------------------- */

  /** Start the resource tracker. */
  void StartResourceTracker(metrics::MetricsComponent component);

  /**
   * End the resource tracker and record the metrics.
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
  void EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id, selfdriving::ExecOUFeatureVector *ouvec);

  /**
   * Initializes an OU feature vector for a given pipeline
   * @param ouvec OU Feature Vector to initialize
   * @param pipeline_id Pipeline to initialize with
   */
  void InitializeOUFeatureVector(selfdriving::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id);

  /**
   * Initializes an OU feature vector for a given parallel step (i.e hashjoin_build, sort_build, agg_build)
   * @param ouvec OU Feature Vector to initialize
   * @param pipeline_id Pipeline to initialize with
   */
  void InitializeParallelOUFeatureVector(selfdriving::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id);

  /* --------------------------------------------------------------------------
    Runtime Parameters (User-Defined Functions)
  -------------------------------------------------------------------------- */

  /** Initialize a new, empty collection of parameters at the top of the parameter stack */
  void StartParams() { runtime_parameters_.emplace(); }

  /** Remove the topmost collection of parameters from the parameter stack */
  void FinishParams() {
    NOISEPAGE_ASSERT(!runtime_parameters_.empty(), "Attempt to pop from empty runtime parameter stack.");
    runtime_parameters_.pop();
  }

  /**
   * Add a runtime parameter to the "top-most" collection of runtime parameters.
   * @param val The parameter to be added
   */
  void AddParam(common::ManagedPointer<sql::Val> val) {
    NOISEPAGE_ASSERT(!runtime_parameters_.empty(), "Must call StartParams() prior to adding runtime parameters.");
    runtime_parameters_.top().push_back(val.CastManagedPointerTo<const sql::Val>());
  }

  /**
   * Add a runtime parameter to the "top-most" collection of runtime parameters.
   * @param val The parameter to be added
   */
  void AddParam(common::ManagedPointer<const sql::Val> val) {
    NOISEPAGE_ASSERT(!runtime_parameters_.empty(), "Must call StartParams() prior to adding runtime parameters.");
    runtime_parameters_.top().push_back(val);
  }

  /**
   * Get the parameter at the specified index.
   * @param index index of parameter to access
   * @return An immutable point to the parameter at specified index
   */
  common::ManagedPointer<const sql::Val> GetParam(uint32_t index) const {
    // Always get the query parameter from the "top-most" collection
    // of parameters; if the runtime parameters stack is empty, default
    // to the "base" set of parameters for the query, otherwise, grab
    // the parameter at the specified index from the top of the runtime
    // parameters stack.
    if (!runtime_parameters_.empty()) {
      NOISEPAGE_ASSERT(index < runtime_parameters_.top().size(), "ExecutionContext::GetParam() index out of range.");
      return runtime_parameters_.top()[index];
    }
    NOISEPAGE_ASSERT(index < parameters_.size(), "ExecutionContext::GetParam() index out of range");
    return parameters_[index];
  }

  /* --------------------------------------------------------------------------
    Other Functionality
  -------------------------------------------------------------------------- */

  /**
   * Constructs a new Output Buffer for outputting query results to consumers.
   * @return The newly created output buffer
   */
  OutputBuffer *OutputBufferNew();

  /**
   * @return On the primary, returns the ID of the last txn sent.
   * On a replica, returns the ID of the last txn applied.
   */
  uint64_t ReplicationGetLastTransactionId() const;

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
   * Ensures that the trackers for the current thread are stopped.
   */
  void EnsureTrackersStopped();

  /**
   * Compute the size of an output tuple based on the provided schema.
   * @param schema The output schema
   * @return The size of tuple in this schema
   */
  static uint32_t ComputeTupleSize(common::ManagedPointer<const planner::OutputSchema> schema);

  /* --------------------------------------------------------------------------
    Hook Function Management
  -------------------------------------------------------------------------- */

  /**
   * Initializes the set of hooks for the execution context to specified capacity.
   * @param num_hooks The desired number of hooks
   */
  void InitHooks(std::size_t num_hooks);

  /**
   * Registers a hook function
   * @param hook_idx Hook index to register function
   * @param hook Function to register
   */
  void RegisterHook(std::size_t hook_idx, HookFn hook);

  /**
   * Invoke a hook function if a hook function is available
   * @param hook_index Index of hook function to invoke
   * @param tls TLS argument
   * @param arg Opaque argument to pass
   */
  void InvokeHook(std::size_t hook_index, void *tls, void *arg);

  /**
   * Clear the hooks for the execution context.
   */
  void ClearHooks() { hooks_.clear(); }

 public:
  /** An empty output schema */
  constexpr static const std::nullptr_t NULL_OUTPUT_SCHEMA{nullptr};
  /** An empty output callback */
  constexpr static const std::nullptr_t NULL_OUTPUT_CALLBACK{nullptr};

 private:
  friend class ExecutionContextBuilder;

  /**
   * Construct a new ExecutionContext instance.
   *
   * NOTE: Private access modifier forces use of ExecutionContextBuilder.
   *
   * @param db_oid The OID of the database
   * @param parameters The query parameters
   * @param execution_settings The execution settings to run with
   * @param txn The transaction used by this query
   * @param output_schema The output schema
   * @param output_callback The callback function for query output
   * @param accessor The catalog accessor of this query
   * @param metrics_manager The metrics manager for recording metrics
   * @param replication_manager The replication manager to handle communication between primary and replicas.
   * @param recovery_manager The recovery manager that handles both recovery and application of replication records.
   */
  ExecutionContext(const catalog::db_oid_t db_oid, std::vector<common::ManagedPointer<const sql::Val>> &&parameters,
                   exec::ExecutionSettings execution_settings,
                   const common::ManagedPointer<transaction::TransactionContext> txn,
                   const common::ManagedPointer<const planner::OutputSchema> output_schema,
                   OutputCallback &&output_callback, const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                   const common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                   const common::ManagedPointer<replication::ReplicationManager> replication_manager,
                   const common::ManagedPointer<storage::RecoveryManager> recovery_manager)
      : db_oid_{db_oid},
        parameters_{std::move(parameters)},
        execution_settings_{execution_settings},
        txn_{txn},
        output_schema_{output_schema},
        output_callback_{std::move(output_callback)},
        accessor_{accessor},
        metrics_manager_{metrics_manager},
        replication_manager_{replication_manager},
        recovery_manager_{recovery_manager},
        mem_tracker_{std::make_unique<sql::MemoryTracker>()},
        mem_pool_{std::make_unique<sql::MemoryPool>(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_))},
        thread_state_container_{std::make_unique<sql::ThreadStateContainer>(mem_pool_.get())} {}

 private:
  /**
   * The query identifier
   *
   * The query identifier is only used in certain situations and is
   * set manually after construction of the ExecutionContext via the
   * SetQueryId() member function.
   */
  query_id_t query_id_{execution::query_id_t(0)};

  /** The OID of the database with which the query is associated */
  const catalog::db_oid_t db_oid_;

  /** The query parameters */
  std::vector<common::ManagedPointer<const execution::sql::Val>> parameters_;
  /** The query execution mode */
  vm::ExecutionMode execution_mode_;
  /** The execution setting for the query */
  const exec::ExecutionSettings execution_settings_;

  /** The associated transaction */
  const common::ManagedPointer<transaction::TransactionContext> txn_;

  /** The query output schema */
  common::ManagedPointer<const planner::OutputSchema> output_schema_{nullptr};
  /** The query output buffer */
  std::unique_ptr<OutputBuffer> buffer_{nullptr};
  /** The query output callback */
  OutputCallback output_callback_;

  /** The query catalog accessor */
  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  /** The query metrics manager */
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  /** The replication manager with which the query is associated */
  common::ManagedPointer<replication::ReplicationManager> replication_manager_;
  /** The recovery manager with which the query is associated */
  common::ManagedPointer<storage::RecoveryManager> recovery_manager_;

  /** The memory tracker */
  std::unique_ptr<sql::MemoryTracker> mem_tracker_;
  /** The memory pool */
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  /** The container for thread-local state */
  std::unique_ptr<sql::ThreadStateContainer> thread_state_container_;

  /** The allocator for strings */
  // TODO(WAN): EXEC PORT we used to push the memory tracker into the string allocator, do this
  sql::VarlenHeap string_allocator_;

  /** The pipeline operating units for the query */
  common::ManagedPointer<selfdriving::PipelineOperatingUnits> pipeline_operating_units_{nullptr};

  /** The number of rows affected by the query */
  uint32_t rows_affected_{0};

  /** `true` if memory overrride is used */
  bool memory_use_override_ = false;
  /** The value to use for memory override */
  uint32_t memory_use_override_value_{0};
  /** The concurrency estimate for query execution */
  uint32_t num_concurrent_estimate_{0};

  /** The hooks for the query */
  std::vector<HookFn> hooks_{};

  /** The query state object */
  void *query_state_;

  /** The runtime parameter stack */
  std::stack<std::vector<common::ManagedPointer<const execution::sql::Val>>> runtime_parameters_;
};

/**
 * The ExecutionContextBuilder class implements a builder for ExecutionContext.
 */
class ExecutionContextBuilder {
 public:
  /**
   * Construct a new ExecutionContextBuilder.
   */
  ExecutionContextBuilder() = default;

  /** @return The completed ExecutionContext instance */
  std::unique_ptr<ExecutionContext> Build();

  /**
   * Set the query parameters for the execution context.
   * @param parameters The query parameters
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithQueryParameters(std::vector<common::ManagedPointer<const sql::Val>> &&parameters) {
    parameters_ = std::move(parameters);
    return *this;
  }

  /**
   * Set the query parameters for the execution context.
   * @param parameter_exprs The collection of expressions from which the query parameters are derived
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithQueryParametersFrom(const std::vector<parser::ConstantValueExpression> &parameter_exprs);

  /**
   * Set the database OID for the execution context.
   * @param db_oid The database OID
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithDatabaseOID(const catalog::db_oid_t db_oid) {
    db_oid_ = db_oid;
    return *this;
  }

  /**
   * Set the transaction context for the execution context.
   * @param txn The transaction context
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithTxnContext(common::ManagedPointer<transaction::TransactionContext> txn) {
    txn_ = txn;
    return *this;
  }

  /**
   * Set the output schema for the execution context.
   * @param output_schema The output schema
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithOutputSchema(common::ManagedPointer<const planner::OutputSchema> output_schema) {
    output_schema_ = output_schema;
    return *this;
  }

  /**
   * Set the output callback for the execution context.
   * @param output_callback The output callback
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithOutputCallback(OutputCallback output_callback) {
    output_callback_.emplace(std::move(output_callback));
    return *this;
  }

  /**
   * Set the catalog accessor for the execution context.
   * @param accessor The catalog accessor
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithCatalogAccessor(common::ManagedPointer<catalog::CatalogAccessor> accessor) {
    catalog_accessor_ = accessor;
    return *this;
  }

  /**
   * Set the execution settings for the execution context.
   * @param exec_settings The execution settings
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithExecutionSettings(exec::ExecutionSettings exec_settings) {
    exec_settings_.emplace(exec_settings);
    return *this;
  }

  /**
   * Set the metrics manager for the execution context.
   * @param metrics_manager The metrics manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithMetricsManager(common::ManagedPointer<metrics::MetricsManager> metrics_manager) {
    metrics_manager_ = metrics_manager;
    return *this;
  }

  /**
   * Set the replication manager for the execution context.
   * @param replication_manager The replication manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithReplicationManager(
      common::ManagedPointer<replication::ReplicationManager> replication_manager) {
    replication_manager_ = replication_manager;
    return *this;
  }

  /**
   * Set the recovery manager for the execution context.
   * @param recovery_manager The recovery manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithRecoveryManager(common::ManagedPointer<storage::RecoveryManager> recovery_manager) {
    recovery_manager_ = recovery_manager;
    return *this;
  }

 private:
  /** The query execution settings */
  std::optional<exec::ExecutionSettings> exec_settings_;
  /** The query parmeters */
  std::vector<common::ManagedPointer<const sql::Val>> parameters_;
  /** The database OID */
  catalog::db_oid_t db_oid_{catalog::INVALID_DATABASE_OID};
  /** The associated transaction */
  std::optional<common::ManagedPointer<transaction::TransactionContext>> txn_;
  /** The output callback */
  std::optional<OutputCallback> output_callback_;
  /** The output schema */
  std::optional<common::ManagedPointer<const planner::OutputSchema>> output_schema_{nullptr};
  /** The catalog accessor */
  std::optional<common::ManagedPointer<catalog::CatalogAccessor>> catalog_accessor_;
  /** The metrics manager */
  std::optional<common::ManagedPointer<metrics::MetricsManager>> metrics_manager_;
  /** The replication manager */
  std::optional<common::ManagedPointer<replication::ReplicationManager>> replication_manager_;
  /** The recovery manager */
  std::optional<common::ManagedPointer<storage::RecoveryManager>> recovery_manager_;
};

}  // namespace noisepage::execution::exec
