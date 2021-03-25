#pragma once

#include <condition_variable>  // NOLINT
#include <queue>
#include <thread>  // NOLINT
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "common/container/concurrent_queue.h"
#include "common/dedicated_thread_task.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace noisepage::replication {
class ReplicationManager;
}  // namespace noisepage::replication

namespace noisepage::storage {

/**
 * Task that processes buffers handed over by transactions and serializes them into consumer buffers.
 * Transactions will wait to be GC'd until their logs are
 */
class LogSerializerTask : public common::DedicatedThreadTask {
 public:
  /**
   * @param serialization_interval             Interval time for when to trigger serialization.
   * @param buffer_pool                        Buffer pool to use to release serialized buffers.
   * @param empty_buffer_queue                 Pointer to queue to pop empty buffers from.
   * @param disk_filled_buffer_queue           Pointer to disk queue to push filled buffers to.
   * @param disk_log_writer_thread_cv          Pointer to cvar to notify consumer when a new buffer has handed over.
   * @param primary_replication_manager        Pointer to replication manager where to-be-replicated serialized logs are
   * sent.
   */
  explicit LogSerializerTask(
      const std::chrono::microseconds serialization_interval, RecordBufferSegmentPool *buffer_pool,
      common::ManagedPointer<common::ConcurrentBlockingQueue<BufferedLogWriter *>> empty_buffer_queue,
      common::ConcurrentQueue<storage::SerializedLogs> *disk_filled_buffer_queue,
      std::condition_variable *disk_log_writer_thread_cv,
      common::ManagedPointer<replication::PrimaryReplicationManager> primary_replication_manager)
      : run_task_(false),
        serialization_interval_(serialization_interval),
        buffer_pool_(buffer_pool),
        disk_filled_buffer_(nullptr),
        replication_filled_buffer_(nullptr),
        empty_buffer_queue_(empty_buffer_queue),
        disk_filled_buffer_queue_(disk_filled_buffer_queue),
        disk_log_writer_thread_cv_(disk_log_writer_thread_cv),
        primary_replication_manager_(primary_replication_manager) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override {
    run_task_ = true;
    LogSerializerTaskLoop();
  }

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override {
    // If the task hasn't run yet, yield the thread until it's started
    while (!run_task_) std::this_thread::yield();
    NOISEPAGE_ASSERT(run_task_, "Cant terminate a task that isnt running");
    run_task_ = false;
  }

  /**
   * Hands a (possibly partially) filled buffer to the serializer task to be serialized
   * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
   * @param retention_policy that decides how the record log will be persisted.
   */
  void AddBufferToFlushQueue(RecordBufferSegment *const buffer_segment, transaction::RetentionPolicy retention_policy) {
    {
      std::unique_lock<std::mutex> guard(flush_queue_latch_);
      if (retention_policy == transaction::RetentionPolicy::RETENTION_LOCAL_DISK_AND_NETWORK_REPLICAS) {
        RecordBufferSegment *buffer_segment_replication = buffer_pool_->Get();
        buffer_segment_replication->Reserve(buffer_segment->size_);
        std::memcpy(buffer_segment_replication->bytes_, buffer_segment->bytes_, common::Constants::BUFFER_SEGMENT_SIZE);
        replication_flush_queue_.push(buffer_segment_replication);
        disk_flush_queue_.push(buffer_segment);
      } else if (retention_policy == transaction::RetentionPolicy::RETENTION_LOCAL_DISK) {
        disk_flush_queue_.push(buffer_segment);
      }

      empty_ = false;
      if (sleeping_) flush_queue_cv_.notify_all();
    }
  }

  /**
   * Set the new log serialization interval in microseconds.
   * @param interval the new serialization interval in microseconds (should > 0)
   */
  void SetSerializationInterval(int32_t interval) { serialization_interval_ = std::chrono::microseconds(interval); }

 private:
  // Marks where a piece of serialized log will be redirected to.
  enum class SerializeDestination { DISK = 0, REPLICAS };

  friend class LogManager;
  // Flag to signal task to run or stop
  bool run_task_;
  // Interval for serialization
  std::chrono::microseconds serialization_interval_;

  // Used to release processed buffers
  RecordBufferSegmentPool *buffer_pool_;

  // Ensures only one thread is serializing at a time.
  common::SpinLatch serialization_latch_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
  //  optimization we applied to the GC queue.
  // Latch to protect flush queue
  std::mutex flush_queue_latch_;
  // Stores unserialized buffers handed off by transactions
  std::queue<RecordBufferSegment *> disk_flush_queue_;
  std::queue<RecordBufferSegment *> replication_flush_queue_;

  // conditional variable to be notified when there are logs to be processed
  std::condition_variable flush_queue_cv_;

  // bools representing whether the logging thread is sleeping and if the log queue is empty
  bool sleeping_ = false, empty_ = true;

  // Current buffer we are serializing logs to
  BufferedLogWriter *disk_filled_buffer_;
  BufferedLogWriter *replication_filled_buffer_;
  // Commit callbacks for commit records currently in filled_buffer
  std::vector<std::pair<transaction::callback_fn, void *>> disk_commits_in_buffer_;
  std::vector<std::pair<transaction::callback_fn, void *>> replication_commits_in_buffer_;

  // Used by the serializer thread to store buffers it has grabbed from the log manager
  std::queue<RecordBufferSegment *> temp_disk_flush_queue_;
  std::queue<RecordBufferSegment *> temp_replication_flush_queue_;

  // We aggregate all transactions we serialize so we can bulk remove the from the timestamp manager
  // TODO(Gus): If we guarantee there is only one TSManager in the system, this can just be a vector. We could also pass
  // TS into the serializer instead of having a pointer for it in every commit/abort record
  std::unordered_map<transaction::TimestampManager *, std::vector<transaction::timestamp_t>> disk_serialized_txns_;
  std::unordered_map<transaction::TimestampManager *, std::vector<transaction::timestamp_t>>
      replication_serialized_txns_;

  // The queue containing empty buffers. Task will dequeue a buffer from this queue when it needs a new buffer
  common::ManagedPointer<common::ConcurrentBlockingQueue<BufferedLogWriter *>> empty_buffer_queue_;
  // The queue containing filled buffers. Task should push filled serialized buffers into this queue
  common::ConcurrentQueue<SerializedLogs> *disk_filled_buffer_queue_;

  // Condition variable to signal disk log consumer task thread that a new full buffer has been pushed to the queue
  std::condition_variable *disk_log_writer_thread_cv_;

  /** The replication manager that serialized log records are shipped to. */
  common::ManagedPointer<replication::PrimaryReplicationManager> primary_replication_manager_;

  /**
   * Main serialization loop. Calls Process every interval. Processes all the accumulated log records and
   * serializes them to log consumer tasks.
   */
  void LogSerializerTaskLoop();

  /**
   * Process all the accumulated log records and serialize them to log consumer tasks. It's important that we serialize
   * the logs in order to ensure that a single transaction's logs are ordered. Only a single thread can serialize the
   * logs (without more sophisticated ordering checks).
   * @return (number of bytes processed, number of records processed, number of transactions processed)
   */
  std::tuple<uint64_t, uint64_t, uint64_t> Process();

  /**
   * Serialize out the task buffer to the current serialization buffer
   * @param buffer_to_serialize the iterator to the redo buffer to be serialized
   * @return tuple representing number of bytes, number of records, and number of txns serialized, used for metrics
   */
  std::tuple<uint64_t, uint64_t, uint64_t> SerializeBuffer(IterableBufferSegment<LogRecord> *buffer_to_serialize,
                                                           SerializeDestination destination);

  /**
   * Serialize out the record to the log
   * @param record the redo record to serialise
   * @param destination of where the serialised record will be directed to
   * @return bytes serialized, used for metrics
   */
  uint64_t SerializeRecord(const LogRecord &record, SerializeDestination destination);

  /**
   * Serialize the data pointed to by val to current serialization buffer
   * @tparam T Type of the value
   * @param val The value to write to the buffer
   * @param destination The destination to write the value to
   * @return bytes written, used for metrics
   */
  template <class T>
  uint32_t WriteValue(const T &val, SerializeDestination destination) {
    return WriteValue(&val, sizeof(T), destination);
  }

  /**
   * Serialize the data pointed to by val to current serialization buffer
   * @param val the value
   * @param size size of the value to serialize
   * @param destination The destination to write the value to
   * @return bytes written, used for metrics
   */
  uint32_t WriteValue(const void *val, uint32_t size, SerializeDestination destination);

  /**
   * Returns the current buffer to serialize logs to
   * @param destination The destination to write the value to
   * @return buffer to write to
   */
  BufferedLogWriter *GetCurrentWriteBuffer(SerializeDestination destination);

  /**
   * Hand over the current buffer and commit callbacks for commit records in that buffer to the log consumer task
   * @param destination The destination to write the value to
   */
  void HandFilledBufferToWriter(SerializeDestination destination);
};
}  // namespace noisepage::storage
