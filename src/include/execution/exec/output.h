#pragma once

#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/execution_common.h"
#include "network/network_defs.h"
#include "parser/parser_defs.h"

namespace terrier::network {
class PostgresPacketWriter;
}  // namespace terrier::network

namespace terrier::planner {
class OutputSchema;
}  // namespace terrier::planner

namespace terrier::execution::exec {

// Callback function
// Params(): tuples, num_tuples, tuple_size;
using OutputCallback = std::function<void(byte *, uint32_t, uint32_t)>;

/**
 * A class that buffers the output and makes a callback for every batch.
 */
class EXPORT OutputBuffer {
 public:
  /**
   * Batch size
   */
  static constexpr uint32_t BATCH_SIZE = 32;

  /**
   * Constructor
   * @param memory_pool memory pool to use for buffer allocation
   * @param num_cols number of columns in output tuples
   * @param tuple_size size of output tuples
   * @param callback upper layer callback
   */
  explicit OutputBuffer(sql::MemoryPool *memory_pool, uint16_t num_cols, uint32_t tuple_size, OutputCallback callback)
      : memory_pool_(memory_pool),
        num_tuples_(0),
        tuple_size_(tuple_size),
        tuples_(
            reinterpret_cast<byte *>(memory_pool->AllocateAligned(BATCH_SIZE * tuple_size, alignof(uint64_t), true))),
        callback_(std::move(callback)) {}

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() {
    if (num_tuples_ == BATCH_SIZE) {
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

  /**
   * @returns tuple size
   */
  uint32_t GetTupleSize() const { return tuple_size_; }

 private:
  sql::MemoryPool *memory_pool_;
  uint32_t num_tuples_;
  uint32_t tuple_size_;
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
  void operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size);

 private:
  uint32_t printed_ = 0;
  const planner::OutputSchema *schema_;
};

/**
 * Output handler for execution engine that writes results to PostgresPacketWriter
 */
class OutputWriter {
 public:
  /**
   * @param schema final schema to output for this query
   * @param out packet writer to use
   * @param field_formats reference to the field formats for this query
   */
  OutputWriter(const common::ManagedPointer<planner::OutputSchema> schema,
               const common::ManagedPointer<network::PostgresPacketWriter> out,
               const std::vector<network::FieldFormat> &field_formats)
      : schema_(schema), out_(out), field_formats_(field_formats) {}

  /**
   * Callback that prints a batch of tuples to std out.
   * @param tuples batch of tuples
   * @param num_tuples number of tuples
   * @param tuple_size size of tuples
   */
  void operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size);

  /**
   * @return number of rows printed
   */
  uint32_t NumRows() const { return num_rows_; }

 private:
  uint32_t num_rows_ = 0;
  const common::ManagedPointer<planner::OutputSchema> schema_;
  const common::ManagedPointer<network::PostgresPacketWriter> out_;
  const std::vector<network::FieldFormat> &field_formats_;
};

/**
 * A consumer that doesn't do anything with the result tuples.
 */
class NoOpResultConsumer {
 public:
  /**
   * Callback that does nothing.
   * @param tuples batch of tuples
   * @param num_tuples number of tuples
   * @param tuple_size size of tuples
   */
  void operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {}
};

}  // namespace terrier::execution::exec
