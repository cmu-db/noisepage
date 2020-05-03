#pragma once

#pragma once
#include <tbb/concurrent_unordered_map.h>
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
#include "execution/util/execution_common.h"
#include "parser/parser_defs.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::network {
class PostgresPacketWriter;
}

namespace terrier::execution::exec {

// Callback function
// Params(): tuples, num_tuples, tuple_size;
using OutputCallback = std::function<void(byte *, uint32_t, uint32_t)>;
static constexpr const int MAX_THREAD_SIZE = 32;
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
        num_tuples_(reinterpret_cast<uint32_t *>(
            memory_pool->AllocateAligned(MAX_THREAD_SIZE * sizeof(uint32_t), alignof(uint32_t), true))),
        tuple_size_(tuple_size),
        id_(0),
        callback_(std::move(callback)) {}

  /**
   * Insert the corresponding buffer of a thread to the map if not created yet
   * @param id The thread ID
   */
  void InsertIfAbsent(std::thread::id id) {
    if (buffer_map_.find(id) == buffer_map_.end()) {
      byte *tuples =
          reinterpret_cast<byte *>(memory_pool_->AllocateAligned(BATCH_SIZE * tuple_size_, alignof(uint64_t), true));
      int index = id_++;
      TERRIER_ASSERT(index <= MAX_THREAD_SIZE, "Thread id is larger than MAX_THREAD_SIZE");
      buffer_map_.insert(std::make_pair(id, std::make_pair(index, tuples)));
    }
  }

  /**
   * @return an output slot to be written to.
   */
  byte *AllocOutputSlot() {
    std::thread::id this_id = std::this_thread::get_id();
    InsertIfAbsent(this_id);
    auto it = buffer_map_.find(this_id);
    size_t index = it->second.first;
    byte *tuples = it->second.second;
    if (num_tuples_[index] == BATCH_SIZE) {
      common::SpinLatch::ScopedSpinLatch guard(&latch_);
      callback_(tuples, num_tuples_[index], tuple_size_);
      num_tuples_[index] = 0;
    }
    // Return the current slot and advance to the to the next one.
    num_tuples_[index]++;
    return tuples + tuple_size_ * (num_tuples_[index] - 1);
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
  uint32_t *num_tuples_;
  uint32_t tuple_size_;
  std::atomic<int> id_;
  // byte *tuples_;
  OutputCallback callback_;
  tbb::concurrent_unordered_map<std::thread::id, std::pair<int, byte *>, std::hash<std::thread::id> > buffer_map_;
  common::SpinLatch latch_;
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
   * @param schema final schema to output
   * @param out packet writer to use
   */
  OutputWriter(const common::ManagedPointer<planner::OutputSchema> schema,
               const common::ManagedPointer<network::PostgresPacketWriter> out)
      : schema_(schema), out_(out) {}

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
  uint64_t NumRows() const { return num_rows_; }

 private:
  uint64_t num_rows_ = 0;
  const common::ManagedPointer<planner::OutputSchema> schema_;
  const common::ManagedPointer<network::PostgresPacketWriter> out_;
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
