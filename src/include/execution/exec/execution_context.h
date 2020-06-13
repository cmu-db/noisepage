#pragma once
#include <memory>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/memory_tracker.h"
#include "execution/util/region.h"
#include "metrics/metrics_defs.h"

namespace terrier::parser {
class ConstantValueExpression;
}  // namespace terrier::parser

namespace terrier::catalog {
class CatalogAccessor;
}  // namespace terrier::catalog

namespace terrier::planner {
class OutputSchema;
}  // namespace terrier::planner

namespace terrier::transaction {
class TransactionContext;
}  // namespace terrier::transaction

namespace terrier::execution::exec {
/**
 * Execution Context: Stores information handed in by upper layers.
 * TODO(Amadou): This class will change once we know exactly what we get from upper layers.
 */
class EXPORT ExecutionContext {
 public:
  /**
   * An allocator for short-ish strings. Needed because the requirements of
   * string allocation (small size and frequent usage) are slightly different
   * than that of generic memory allocator for larger structures. This string
   * allocator relies on memory regions for fast allocations, and bulk
   * deletions.
   */
  class StringAllocator {
   public:
    /**
     * Create a new allocator
     */
    explicit StringAllocator(common::ManagedPointer<sql::MemoryTracker> tracker) : region_(""), tracker_(tracker) {}

    /**
     * This class cannot be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(StringAllocator);

    /**
     * Destroy allocator
     */
    ~StringAllocator() = default;

    /**
     * Allocate a string of the given size..
     * @param size Size of the string in bytes.
     * @return A pointer to the string.
     */
    char *Allocate(std::size_t size);

    /**
     * No-op. Bulk de-allocated upon destruction.
     */
    void Deallocate(char *str) {}

   private:
    util::Region region_;
    // Metadata tracker for memory allocations
    common::ManagedPointer<sql::MemoryTracker> tracker_;
  };

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
                   common::ManagedPointer<catalog::CatalogAccessor> accessor);

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
  StringAllocator *GetStringAllocator() { return &string_allocator_; }

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

 private:
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unique_ptr<sql::MemoryTracker> mem_tracker_;
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  std::unique_ptr<OutputBuffer> buffer_;
  StringAllocator string_allocator_;
  common::ManagedPointer<brain::PipelineOperatingUnits> pipeline_operating_units_;
  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> params_;
  uint8_t execution_mode_;
  uint64_t rows_affected_ = 0;
};
}  // namespace terrier::execution::exec
