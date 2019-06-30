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
#include "execution/util/common.h"
#include "planner/plannodes/output_schema.h"

namespace tpl::exec {

// Assumes the user of the callback knows the output schema
// So it can get read attributes itself.
// Params: tuples, null_bitmap, num_tuples, ;
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
  explicit OutputBuffer(u16 num_cols, u32 tuple_size, OutputCallback callback)
      : num_tuples_(0),
        tuple_size_(tuple_size),
        tuples_(new byte[batch_size_ * tuple_size]),
        callback_(std::move(callback)) {}

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() { return tuples_ + tuple_size_ * num_tuples_; }

  /**
   * Advances the slot
   */
  void Advance();

  /**
   * Sets a particular tuple to null depending on value
   * TODO(Amadou): Remove if unneeded.
   * @param col_idx index of column
   * @param value whether to set to null or not
   */
  void SetNull(u16 col_idx, bool value) {}

  /**
   * Called at the end of execution to return the final few tuples.
   */
  void Finalize();

  /**
   * Destructor
   */
  ~OutputBuffer();

 private:
  u32 num_tuples_;
  u32 tuple_size_;
  byte *tuples_;
  OutputCallback callback_;
};

/**
 * Only For Debugging.
 * A OutputCallback that prints tuples to std::cout.
 */
class OutputPrinter {
 public:
  /**
   * Constructor
   * @param schema final schema to output
   */
  explicit OutputPrinter(const terrier::planner::OutputSchema *schema) : schema_(schema) {}

  /**
   * Callback that prints a batch of tuples to std out.
   * @param tuples batch of tuples
   * @param num_tuples number of tuples
   * @param tuple_size size of tuples
   */
  void operator()(byte *tuples, u32 num_tuples, u32 tuple_size);

 private:
  uint32_t printed_ = 0;
  const terrier::planner::OutputSchema *schema_;
};

}  // namespace tpl::exec
