#pragma once

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "network/network_io_utils.h"
#include "replication/replication_messages.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::storage {

/**
 * Log provider for the logs being received over network.
 * Provides logs to the recovery manager from logs being sent by master node over the network.
 */
class ReplicationLogProvider final : public AbstractLogProvider {
 public:
  LogProviderType GetType() const override { return LogProviderType::REPLICATION; }

  /** Notify the log provider that replication is ending. This signals all relevant condition variables. */
  void EndReplication() {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_active_ = false;
    replication_cv_.notify_all();
  }

  /** Add the batch of records to the log provider. */
  void AddBatchOfRecords(const replication::RecordsBatchMsg &msg) {
    replication_latch_.lock();
    received_batch_queue_.emplace(msg);
    replication_latch_.unlock();
    replication_cv_.notify_all();
  }

  /** @return True if there are more records. False otherwise. */
  bool NonBlockingHasMoreRecords() const {
    return (curr_buffer_ != nullptr && curr_buffer_->HasMore()) || !received_batch_queue_.empty();
  }

 private:
  /** @return True if left > right. False otherwise. */
  static bool CompareBatches(const replication::RecordsBatchMsg &left, const replication::RecordsBatchMsg &right) {
    return left.GetBatchId() > right.GetBatchId();
  }

  /** @return True if the next batch has arrived. Assumes replication_latch_ is held. */
  bool NextBatchReady() {
    // If there are no batches, then the next batch certainly has not arrived.
    if (received_batch_queue_.empty()) {
      return false;
    }
    // The next batch is ready for application if no batch has been applied yet or if the batch ID is consecutive.
    bool no_batch_applied_yet = last_batch_popped_ == replication::INVALID_RECORD_BATCH_ID;
    bool top_batch_is_next =
        !received_batch_queue_.empty() &&
        received_batch_queue_.top().GetBatchId() == replication::RecordsBatchMsg::NextBatchId(last_batch_popped_);
    return no_batch_applied_yet || top_batch_is_next;
  }

  /**
   * @return        If replication has ended, false.
   *                If there are more log records that can be immediately applied, true.
   *                If there are no such log records and replication has not yet ended, blocks.
   */
  bool HasMoreRecords() override {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_cv_.wait(lock,
                         [&] { return !replication_active_ || (NonBlockingHasMoreRecords() && NextBatchReady()); });
    return replication_active_;
  }

  /**
   * Read data from the log file into the destination provided.
   * @param dest    Pointer to location to read into.
   * @param size    Number of bytes to read.
   * @return        True if the given number of bytes were read. False otherwise.
   */
  bool Read(void *dest, uint32_t size) override {
    if (curr_buffer_ == nullptr || !curr_buffer_->HasMore()) {
      std::unique_lock<std::mutex> lock(replication_latch_);
      replication_cv_.wait(lock, [&] { return !replication_active_ || NextBatchReady(); });
      // Check if replication has shut down.
      if (!replication_active_) return false;

      // Pop the next batch of records off into curr_buffer_.
      {
        const replication::RecordsBatchMsg &msg = received_batch_queue_.top();
        std::string contents = msg.GetContents();
        std::vector<unsigned char> bytes(contents.begin(), contents.end());
        network::ReadBufferView view(bytes.size(), bytes.begin());
        auto buffer = std::make_unique<network::ReadBuffer>();
        buffer->FillBufferFrom(view, bytes.size());

        NOISEPAGE_ASSERT((last_batch_popped_ == replication::INVALID_RECORD_BATCH_ID) ||
                             (msg.GetBatchId() == replication::RecordsBatchMsg::NextBatchId(last_batch_popped_)),
                         "Batches are being added out of order?");

        last_batch_popped_ = msg.GetBatchId();
        curr_buffer_ = std::move(buffer);
        received_batch_queue_.pop();
        replication_cv_.notify_one();
      }
    }

    // Read in as much as is available in this buffer.
    auto readable_size = curr_buffer_->HasMore(size) ? size : curr_buffer_->BytesAvailable();
    curr_buffer_->ReadIntoView(readable_size).Read(readable_size, dest);

    // If there is more data to read, recursively call Read until all of the data is read.
    return (readable_size < size) ? Read(static_cast<char *>(dest) + readable_size, size - readable_size) : true;
  }

  bool replication_active_ = true;  ///< True if replication is currently active. False otherwise.
  std::unique_ptr<network::ReadBuffer> curr_buffer_ = nullptr;  ///< Current buffer to read logs from.

  /** The batches received from replication. */
  std::priority_queue<replication::RecordsBatchMsg, std::vector<replication::RecordsBatchMsg>,
                      std::function<bool(replication::RecordsBatchMsg, replication::RecordsBatchMsg)>>
      received_batch_queue_{CompareBatches};
  replication::record_batch_id_t last_batch_popped_ = replication::INVALID_RECORD_BATCH_ID;

  /** Synchronizes received_batch_queue_ and process termination. */
  ///@{
  std::mutex replication_latch_;
  std::condition_variable replication_cv_;
  ///@}
};

}  // namespace noisepage::storage
