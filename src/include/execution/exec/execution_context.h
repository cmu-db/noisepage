#pragma once
#include <memory>
#include <utility>
#include "execution/exec/output.h"
#include "execution/sql/memory_pool.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "planner/plannodes/output_schema.h"
#include "execution/util/region.h"

namespace tpl::exec {
using terrier::transaction::TransactionContext;

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
   * @param txn transaction used by this query
   * @param callback callback function for outputting
   * @param schema the schema of the output
   */
  ExecutionContext(TransactionContext *txn, const OutputCallback & callback, const terrier::planner::OutputSchema *schema)
  : txn_(txn),
    buffer_(schema == nullptr ? nullptr : std::make_unique<OutputBuffer>(schema->GetColumns().size(),
                                                                         ComputeTupleSize(schema), callback)) {}

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

  StringAllocator * GetStringAllocator() {
    return &string_allocator_;
  }

  /**
   * @param schema the schema of the output
   * @return the size of tuple with this final_schema
   */
  static uint32_t ComputeTupleSize(const terrier::planner::OutputSchema *schema);

 private:
  TransactionContext *txn_;
  std::unique_ptr<OutputBuffer> buffer_;
  std::unique_ptr<sql::MemoryPool> mem_pool_{nullptr};
  StringAllocator string_allocator_;
};
}  // namespace tpl::exec
