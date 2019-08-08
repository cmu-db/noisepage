#pragma once
#include <memory>
#include <utility>
#include "catalog/catalog_accessor.h"
#include "execution/exec/output.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/region.h"
#include "planner/plannodes/output_schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::execution::exec {
using transaction::TransactionContext;

/**
 * Execution Context: Stores information handed in by upper layers.
 */
class ExecutionContext {
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
    StringAllocator();

    /**
     * This class cannot be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(StringAllocator);

    /**
     * Destroy allocator
     */
    ~StringAllocator();

    /**
     * Allocate a string of the given size..
     * @param size Size of the string in bytes.
     * @return A pointer to the string.
     */
    char *Allocate(std::size_t size);

    /**
     * Deallocate a string allocated from this allocator.
     * @param str The string to deallocate.
     */
    void Deallocate(char *str);

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
  ExecutionContext(catalog::db_oid_t db_oid, TransactionContext *txn, const OutputCallback &callback,
                   const planner::OutputSchema *schema, std::unique_ptr<catalog::CatalogAccessor> &&accessor)
      : db_oid_(db_oid),
        txn_(txn),
        buffer_(schema == nullptr
                    ? nullptr
                    : std::make_unique<OutputBuffer>(schema->GetColumns().size(), ComputeTupleSize(schema), callback)),
        accessor_(std::move(accessor)) {}

  /**
   * @return the transaction used by this query
   */
  TransactionContext *GetTxn() { return txn_; }

  /**
   * @return the output buffer used by this query
   */
  OutputBuffer *GetOutputBuffer() { return buffer_.get(); }

  /**
   * @return the memory pool
   */
  sql::MemoryPool *GetMemoryPool() { return mem_pool_.get(); }

  /**
   * Set the memory pool
   * @param mem_pool new memory pool
   */
  void SetMemoryPool(std::unique_ptr<sql::MemoryPool> &&mem_pool) { mem_pool_ = std::move(mem_pool); }

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
  catalog::CatalogAccessor *GetAccessor() { return accessor_.get(); }

  /**
   * @return the db oid
   */
  catalog::db_oid_t DBOid() { return db_oid_; }

 private:
  catalog::db_oid_t db_oid_;
  TransactionContext *txn_;
  std::unique_ptr<OutputBuffer> buffer_;
  std::unique_ptr<sql::MemoryPool> mem_pool_{nullptr};
  StringAllocator string_allocator_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
};
}  // namespace terrier::execution::exec
