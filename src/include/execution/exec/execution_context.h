#pragma once
#include <memory>
#include <utility>
#include <vector>

// TODO(WAN): COMPILE TIME looks like a lot of possible forward declarations

#include "catalog/catalog_accessor.h"
#include "common/managed_pointer.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/memory_tracker.h"
#include "execution/sql/runtime_types.h"
#include "execution/util/region.h"
#include "metrics/metrics_defs.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::brain {
class PipelineOperatingUnits;
}

namespace terrier::catalog {
class CatalogAccessor;
}

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
   */
  ExecutionContext(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                   const OutputCallback &callback, const planner::OutputSchema *schema,
                   const common::ManagedPointer<catalog::CatalogAccessor> accessor)
      : db_oid_(db_oid),
        txn_(txn),
        mem_tracker_(std::make_unique<sql::MemoryTracker>()),
        mem_pool_(std::make_unique<sql::MemoryPool>(common::ManagedPointer<sql::MemoryTracker>(mem_tracker_))),
        buffer_(schema == nullptr ? nullptr
                                  : std::make_unique<OutputBuffer>(mem_pool_.get(), schema->GetColumns().size(),
                                                                   ComputeTupleSize(schema), callback)),
        accessor_(accessor),
        select_opt_threshold_{common::Constants::SELECT_OPT_THRESHOLD},
        arithmetic_full_compute_opt_threshold_{common::Constants::ARITHMETIC_FULL_COMPUTE_THRESHOLD},
        min_bit_density_threshold_for_AVX_index_decode_{common::Constants::BIT_DENSITY_THRESHOLD_FOR_AVX_INDEX_DECODE},
        adaptive_predicate_order_sampling_frequency_{common::Constants::ADAPTIVE_PRED_ORDER_SAMPLE_FREQ},
        parallel_query_execution_{common::Constants::IS_PARALLEL_QUERY_EXECUTION} {}

  /**
   * @return the transaction used by this query
   */
  common::ManagedPointer<transaction::TransactionContext> GetTxn() { return txn_; }

  /**
   * @return the output buffer used by this query
   */
  OutputBuffer *GetOutputBuffer() { return buffer_.get(); }

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
   * @return the catalog accessor
   */
  catalog::CatalogAccessor *GetAccessor() { return accessor_.Get(); }

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
   * End the resource tracker for a pipeline and record the metrics
   * @param query_id query identifier
   * @param pipeline_id id of the pipeline
   */
  void EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id);

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
   * @param params The exection parameters.
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

  /**
   * @return The vector active element threshold past which full auto-vectorization is done on vectors
   */
  const constexpr double GetSelectOptThreshold() { return select_opt_threshold_; }

  /**
   * @return The vector selectivity past which full computation is done
   */
  const constexpr double GetArithmeticFullComputeOptThreshold() { return arithmetic_full_compute_opt_threshold_; }

  /**
   * @return The minimum bit vector density before using a SIMD decoding algorithm
   */
  const constexpr float GetMinBitDensityThresholdForAVXIndexDecode() {
    return min_bit_density_threshold_for_AVX_index_decode_;
  }

  /**
   * @return The frequency at which to sample statistics when adaptively reordering
   * predicate clauses
   */
  const constexpr float GetAdaptivePredicateOrderSamplingFrequency() {
    return adaptive_predicate_order_sampling_frequency_;
  }

  /**
   * @return Whether or not parallel query execution is being used
   */
  bool GetIsParallelQueryExecution() { return parallel_query_execution_; }

 private:
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unique_ptr<sql::MemoryTracker> mem_tracker_;
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  std::unique_ptr<OutputBuffer> buffer_;
  // TODO(WAN): EXEC PORT we used to push the memory tracker into the string allocator, do this
  sql::VarlenHeap string_allocator_;
  common::ManagedPointer<brain::PipelineOperatingUnits> pipeline_operating_units_;
  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> params_;
  uint8_t execution_mode_;
  uint64_t rows_affected_ = 0;

  // hardcoded for now
  double select_opt_threshold_;
  double arithmetic_full_compute_opt_threshold_;
  float min_bit_density_threshold_for_AVX_index_decode_;
  float adaptive_predicate_order_sampling_frequency_;
  bool parallel_query_execution_{false};
};
}  // namespace terrier::execution::exec
