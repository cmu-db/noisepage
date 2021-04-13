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
  /** ReplicationEvent describes what kind of action should happen next for the log consumer. */
  enum class ReplicationEvent : uint8_t {
    END = 0,  ///< Replication ended.
    LOGS,     ///< Unprocessed replication logs (ReplicateBatchMsg) available.
    OAT       ///< Unprocessed oldest active txn (NotifyOATMsg) available.
  };

  LogProviderType GetType() const override { return LogProviderType::REPLICATION; }

  /** Notify the log provider that replication is ending. This signals all relevant condition variables. */
  void EndReplication() {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_active_ = false;
    replication_cv_.notify_all();
  }

  /** Add the batch of records to the log provider. */
  void AddBatchOfRecords(const replication::RecordsBatchMsg &msg) {
    {
      std::unique_lock<std::mutex> lock(replication_latch_);
      received_batch_queue_.emplace(msg);
    }
    replication_cv_.notify_all();
  }

  /** @return True if there are more records. False otherwise. */
  bool NonBlockingHasMoreRecords() const {
    return (curr_buffer_ != nullptr && curr_buffer_->HasMore()) || !received_batch_queue_.empty();
  }

  /** @return True if there is an unprocessed OAT that is ready to be applied. See docs/design_replication.md. */
  bool OATReady() const {
    bool currently_reading_buffer = curr_buffer_ != nullptr && curr_buffer_->HasMore();
    bool all_batches_popped = !oats_.empty() && oats_.top().batch_id_ <= last_batch_popped_;
    return !currently_reading_buffer && all_batches_popped;
  }

  /**
   * Block until a replication event is available; either replication ended, logs are received, or an OAT is available.
   * @return The type of replication event that became available.
   */
  ReplicationEvent WaitUntilEvent() {
    std::unique_lock<std::mutex> lock(replication_latch_);
    while (true) {
      // The order here matters. Due to the AbstractLogProvider framework that was inherited, returning LOGS without
      // having all of the replication logs currently available may cause the log consumer to block.
      // Moreover, check before blocking that there is really nothing to do, as cvar signals may have been missed.
      if (!replication_active_) return ReplicationEvent::END;
      if (OATReady()) return ReplicationEvent::OAT;
      if (NonBlockingHasMoreRecords() || NextBatchReady()) return ReplicationEvent::LOGS;
      replication_cv_.wait(
          lock, [&] { return !replication_active_ || NonBlockingHasMoreRecords() || NextBatchReady() || OATReady(); });
    }
  }

  /**
   * Update the latest OAT of the replication log provider.
   *
   * The OAT is used to signal to a replication log consumer, e.g., recovery, that all transactions up to and including
   * OAT are now safe to be applied, as long as all replication batches up to batch ID have been processed.
   *
   * @param oldest_active_txn The last transaction inclusive that is safe to be applied, once batches are processed.
   * @param batch_id The last batch ID that must have been processed before the OAT can be applied.
   */
  void UpdateOAT(transaction::timestamp_t oldest_active_txn, replication::record_batch_id_t batch_id) {
    std::unique_lock lock(replication_latch_);
    oats_.emplace(OATPair{oldest_active_txn, batch_id});
    replication_cv_.notify_all();
  }

  /** @return The latest OAT to be applied, popped off the replication log. */
  transaction::timestamp_t PopOAT() {
    std::unique_lock lock(replication_latch_);
    NOISEPAGE_ASSERT(!oats_.empty(),
                     "PopOAT() should only be invoked in response to WaitForEvent() telling the caller that there is "
                     "an OAT available. If there is no OAT available, then why is this function being invoked?");
    transaction::timestamp_t oat = oats_.top().oat_;
    oats_.pop();
    replication_cv_.notify_all();
    return oat;
  }

 private:
  /** @return True if left > right. False otherwise. */
  static bool CompareBatches(const replication::RecordsBatchMsg &left, const replication::RecordsBatchMsg &right) {
    return left.GetBatchId() > right.GetBatchId();
  }

  /** A pair of OAT and associated batch ID that must be processed before the OAT. */
  struct OATPair {
    transaction::timestamp_t oat_;             ///< The last transaction inclusive that is safe to be applied.
    replication::record_batch_id_t batch_id_;  ///< The last batch inclusive that must be processed before applying OAT.
  };

  /** @return True if left > right. False otherwise. */
  static bool CompareOATs(const OATPair &left, const OATPair &right) { return left.batch_id_ > right.batch_id_; }

  /** @return True if the next batch has arrived. Assumes replication_latch_ is held. */
  bool NextBatchReady() {
    // If there are no batches, then the next batch certainly has not arrived.
    if (received_batch_queue_.empty()) {
      return false;
    }
    NOISEPAGE_ASSERT(received_batch_queue_.top().GetBatchId() > last_batch_popped_, "Duplicate batch added?");
    // The next batch is ready for application if the batch ID is consecutive.
    bool top_batch_is_next =
        received_batch_queue_.top().GetBatchId() == replication::RecordsBatchMsg::NextBatchId(last_batch_popped_);
    return top_batch_is_next;
  }

  /**
   * @return        If replication has ended, false.
   *                If there are more log records that can be immediately applied, true.
   *                If there are no such log records and replication has not yet ended, blocks.
   */
  bool HasMoreRecords() override {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_cv_.wait(lock, [&] { return !replication_active_ || NonBlockingHasMoreRecords() || NextBatchReady(); });
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
  std::priority_queue<OATPair, std::vector<OATPair>, std::function<bool(OATPair, OATPair)>> oats_{CompareOATs};

  /** Synchronizes received_batch_queue_ and process termination. */
  ///@{
  std::mutex replication_latch_;
  std::condition_variable replication_cv_;
  ///@}
};

}  // namespace noisepage::storage
