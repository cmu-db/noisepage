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
#include "execution/sql/projected_columns_iterator.h"
#include "execution/sql/value.h"
#include "execution/util/chunked_vector.h"

namespace tpl::exec {
using terrier::catalog::Schema;

// Assumes the user of the callback knows the output schema
// So it can get read attributes itself.
// Params: tuples, null_bitmap, num_tuples, ;
using OutputCallback = std::function<void(byte *, u32, u32)>;

/**
 * The final schema outputted to the upper layer
 */
class FinalSchema {
 public:
  FinalSchema(std::vector<Schema::Column> cols, std::unordered_map<u32, u32> offsets)
      : cols_(std::move(cols)), offsets_(std::move(offsets)) {}

  const std::vector<Schema::Column> &GetCols() const { return cols_; }

  u32 GetOffset(u32 idx) const { return offsets_.at(idx); }

 private:
  const std::vector<Schema::Column> cols_;
  const std::unordered_map<u32, u32> offsets_;
};

/// A class that buffers the output and makes a callback for every batch.
class OutputBuffer {
 public:
  static constexpr uint32_t batch_size_ = 32;
  explicit OutputBuffer(u16 num_cols, u32 tuple_size, OutputCallback callback)
      : num_tuples_(0),
        tuple_size_(tuple_size),
        tuples_(new byte[batch_size_ * tuple_size]),
        callback_(std::move(callback)) {}

  // Returns an output slot to be written to.
  byte *AllocOutputSlot() { return tuples_ + tuple_size_ * num_tuples_; }

  // Advances the slot
  void Advance();

  // TODO(Amadou): Remove if unneeded.
  void SetNull(u16 col_idx, bool value) {}

  // Called at the end to return the final few tuples.
  void Finalize();

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
  explicit OutputPrinter(const FinalSchema &schema) : schema_(schema) {}

  // Prints the tuples in this batch.
  void operator()(byte *tuples, u32 num_tuples, u32 tuple_size);

 private:
  uint32_t printed_ = 0;
  const FinalSchema &schema_;
};

}  // namespace tpl::exec
