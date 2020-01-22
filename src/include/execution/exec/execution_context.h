#pragma once
#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/managed_pointer.h"
#include "execution/exec/output.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/region.h"
#include "planner/plannodes/output_schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/transient_value.h"

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
    StringAllocator() : region_("") {}

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
                   const common::ManagedPointer<catalog::CatalogAccessor> accessor)
      : db_oid_(db_oid),
        txn_(txn),
        mem_pool_(std::make_unique<sql::MemoryPool>(nullptr)),
        buffer_(schema == nullptr ? nullptr
                                  : std::make_unique<OutputBuffer>(mem_pool_.get(), schema->GetColumns().size(),
                                                                   ComputeTupleSize(schema), callback)),
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
   * @return the db oid
   */
  catalog::db_oid_t DBOid() { return db_oid_; }

  /**
   * Set the accessor
   * @param accessor The catalog accessor.
   */
  void SetAccessor(const common::ManagedPointer<catalog::CatalogAccessor> accessor) { accessor_ = accessor; }

  /**
   * Set the execution parameters.
   * @param params The exection parameters.
   */
  void SetParams(std::vector<type::TransientValue> &&params) { params_ = std::move(params); }

  /**
   * @param param_idx index of parameter to access
   * @return immutable parameter at provided index
   */
  const type::TransientValue &GetParam(uint32_t param_idx) const { return params_[param_idx]; }

  /**
   * INSERT, UPDATE, and DELETE queries return a number for the rows affected, so this should be incremented in the root
   * nodes of the query
   */
  uint64_t &RowsAffected() { return rows_affected_; }

 private:
  catalog::db_oid_t db_oid_;
  common::ManagedPointer<transaction::TransactionContext> txn_;
  std::unique_ptr<sql::MemoryPool> mem_pool_;
  std::unique_ptr<OutputBuffer> buffer_;
  StringAllocator string_allocator_;
  common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  std::vector<type::TransientValue> params_;
  uint64_t rows_affected_ = 0;
};
}  // namespace terrier::execution::exec
