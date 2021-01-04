#pragma once

#include <string>

#include "network/network_io_utils.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::storage {

/**
 * Log provider for logs being received over network.
 * Provides logs to the recovery manager from logs being sent by master node over the network.
 */
class ReplicationLogProvider final : public AbstractLogProvider {
 public:
  ReplicationLogProvider(std::chrono::seconds replication_timeout, bool synchronous_replication)
      : replication_active_(true),
        replication_timeout_(replication_timeout),
        synchronous_replication_(synchronous_replication) {}

  /**
   * Notifies the log provider that replication is ending.
   */
  void EndReplication() {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_active_ = false;
    replication_cv_.notify_one();
  }

  /**
   * Hands a buffer of log records to the replication provider.
   * @param buffer buffer of log records.
   */
  void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
    std::unique_lock<std::mutex> lock(replication_latch_);
    arrived_buffer_queue_.emplace(std::move(buffer));
    replication_cv_.notify_one();
  }

  // buffer the message
  void AddBufferFromMessage(const std::string &content) {
    std::vector<unsigned char> bytes(content.begin(), content.end());
    network::ReadBufferView view(bytes.size(), bytes.begin());
    auto buffer = std::make_unique<network::ReadBuffer>();
    buffer->FillBufferFrom(view, bytes.size());
    {
      std::unique_lock<std::mutex> lock(replication_latch_);
      arrived_buffer_queue_.emplace(std::move(buffer));
      replication_cv_.notify_one();
    }

    // TODO(WAN): if you want sync/async I think this is where you do it
  }

  // TODO(gus): this method is not entirely correct, there could still be messages over network
  /**
   * TODO(WAN): Per Gus, tThis method is not entirely correct because there could still be messages pending over
   *  the network.
   */
  void WaitUntilSync() {
    while ((curr_buffer_ != nullptr && curr_buffer_->HasMore()) || !arrived_buffer_queue_.empty())
      std::this_thread::yield();
    STORAGE_LOG_INFO("Replica Synced");
  }

  bool NonBlockingHasMoreRecords() const {
    return (curr_buffer_ != nullptr && curr_buffer_->HasMore()) || !arrived_buffer_queue_.empty();
  }

  bool IsSynchronousReplication() const { return synchronous_replication_; }

 private:
  /** True if replication is currently active. */
  bool replication_active_;

  // TODO(Gus): Put in settings manager
  /** The number of seconds before replication times out. */
  std::chrono::seconds replication_timeout_;

  // True if synchronous replication is enabled
  bool synchronous_replication_;

  // Current buffer to read logs from
  std::unique_ptr<network::ReadBuffer> curr_buffer_ = nullptr;

  // Buffers that have arrived in packets from the master node
  // TODO(Gus): For now, these buffers will most likely be allocated on demand by the network layer. We could consider
  // having a buffer pool if allocation becomes a bottleneck
  std::queue<std::unique_ptr<network::ReadBuffer>> arrived_buffer_queue_;

  // Synchronisation primitives to synchronise arrival of packets and process termination
  std::mutex replication_latch_;
  std::condition_variable replication_cv_;

  /**
   * @return true if log file contains more records, false otherwise. Can be blocking if waiting for message from
   * master, or master has timed out.
   */
  bool HasMoreRecords() override {
    std::unique_lock<std::mutex> lock(replication_latch_);
    // We wake up from CV if:
    //  1. We time out
    //  2. Someone ends replication
    //  3. We have a current buffer and it has more bytes availible
    //  4. A new packet has arrived
    // TODO(WAN): asan on shutdown due to replication_active_, look at teardown order?
    bool predicate = replication_cv_.wait_for(lock, replication_timeout_,
                                              [&] { return !replication_active_ || NonBlockingHasMoreRecords(); });
    // If we time out, predicate == false
    return predicate && replication_active_;
  }

  /**
   * Read data from the log file into the destination provided.
   * @param dest pointer to location to read into.
   * @param size number of bytes to read.
   * @return true if we read the given number of bytes.
   */
  bool Read(void *dest, uint32_t size) override {
    if (curr_buffer_ == nullptr || !curr_buffer_->HasMore()) {
      std::unique_lock<std::mutex> lock(replication_latch_);
      bool predicate = replication_cv_.wait_for(lock, replication_timeout_,
                                                [&] { return !replication_active_ || !arrived_buffer_queue_.empty(); });
      // If we timeout or replication is shut down, return false
      if (!predicate || !replication_active_) return false;

      NOISEPAGE_ASSERT(!arrived_buffer_queue_.empty(),
                       "If we did not shut down or timeout, CV should only wake up when a new buffer arrives");
      curr_buffer_ = std::move(arrived_buffer_queue_.front());
      arrived_buffer_queue_.pop();
    }

    // Read in as much as as is availible in this buffer.
    auto readable_size = curr_buffer_->HasMore(size) ? size : curr_buffer_->BytesAvailable();
    curr_buffer_->ReadIntoView(readable_size).Read(readable_size, dest);

    // If we didn't read all that we needed to, call Read again to read the rest of the data
    // We cast dest to char* to increase pointer by the size we read.
    return (readable_size < size) ? Read(static_cast<char *>(dest) + readable_size, size - readable_size) : true;
  }
};

}  // namespace noisepage::storage