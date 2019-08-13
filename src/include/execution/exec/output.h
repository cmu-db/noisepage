#pragma once

#pragma once
#include <iostream>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/common.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::execution::exec {

// Callback function
// Params: tuples, num_tuples, tuple_size;
using OutputCallback = std::function<void(byte *, u32, u32)>;

/**
 * A class that buffers the output and makes a callback for every batch.
 */
class OutputBuffer {
 public:
  /**
   * Batch size
   */
  static constexpr uint32_t batch_size_ = 32;

  /**
   * Constructor
   * @param num_cols number of columns in output tuples
   * @param tuple_size size of output tuples
   * @param callback upper layer callback
   */
  explicit OutputBuffer(sql::MemoryPool *memory_pool, u16 num_cols, u32 tuple_size, OutputCallback callback)
      : memory_pool_(memory_pool),
        num_tuples_(0),
        tuple_size_(tuple_size),
        tuples_(reinterpret_cast<byte *>(memory_pool->AllocateAligned(batch_size_ * tuple_size, sizeof(u64), false))),
        callback_(std::move(callback)) {}

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() {
    if (num_tuples_ == batch_size_) {
      callback_(tuples_, num_tuples_, tuple_size_);
      num_tuples_ = 0;
    }
    // Return the current slot and advance to the to the next one.
    num_tuples_++;
    return tuples_ + tuple_size_ * (num_tuples_ - 1);
  }

  /**
   * Called at the end of execution to return the final few tuples.
   */
  void Finalize();

  /**
   * Destructor
   */
  ~OutputBuffer();

 private:
  sql::MemoryPool *memory_pool_;
  u32 num_tuples_;
  u32 tuple_size_;
  byte *tuples_;
  OutputCallback callback_;
};

/**
 * Only For Debugging.
 * A OutputCallback that prints tuples to standard out.
 */
class OutputPrinter {
 public:
  /**
   * Constructor
   * @param schema final schema to output
   */
  explicit OutputPrinter(const planner::OutputSchema *schema) : schema_(schema) {}

  /**
   * Callback that prints a batch of tuples to std out.
   * @param tuples batch of tuples
   * @param num_tuples number of tuples
   * @param tuple_size size of tuples
   */
  void operator()(byte *tuples, u32 num_tuples, u32 tuple_size);

 private:
  uint32_t printed_ = 0;
  const planner::OutputSchema *schema_;
};

}  // namespace terrier::execution::exec
