#pragma once

#include <string>
#include "network/network_io_utils.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::storage {

/**
 * @brief Log provider for logs being received over network
 * Provides logs to the recovery manager from logs being sent by master node over the network
 */
<<<<<<< HEAD
  class ReplicationLogProvider final : public AbstractLogProvider {
  public:
    ReplicationLogProvider(std::chrono::seconds replication_timeout)
        : replication_active_(true), replication_timeout_(replication_timeout) {}

    /**
     * Notifies the log provider that replication is ending
     */
    void EndReplication() {
      std::unique_lock<std::mutex> lock(replication_latch_);
      replication_active_ = false;
      replication_cv_.notify_one();
    }

    /**
     * Hands a buffer of log records to the replication provider
     * @param buffer buffer of log records
     */
    void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
      std::unique_lock<std::mutex> lock(replication_latch_);
      arrived_buffer_queue_.emplace(std::move(buffer));
      replication_cv_.notify_one();
    }

  private:
    // True if replication is active
    bool replication_active_;

    // TODO(Gus): Put in settings manager
    std::chrono::seconds replication_timeout_;

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
     * master, or master has timed out
     */
    bool HasMoreRecords() override {
      std::unique_lock<std::mutex> lock(replication_latch_);
      // We wake up from CV if:
      //  1. We time out
      //  2. Someone ends replication
      //  3. We have a current buffer and it has more bytes availible
      //  4. A new packet has arrived
      bool predicate = replication_cv_.wait_for(lock, replication_timeout_, [&] {
        return !replication_active_ || (curr_buffer_ != nullptr && curr_buffer_->HasMore()) ||
               !arrived_buffer_queue_.empty();
      });
      // If we time out, predicate == false
      return predicate && replication_active_;
    }

    /**
     * Read data from the log file into the destination provided
     * @param dest pointer to location to read into
     * @param size number of bytes to read
     * @return true if we read the given number of bytes
     */
    bool Read(void *dest, uint32_t size) override {
      if (curr_buffer_ == nullptr || !curr_buffer_->HasMore()) {
        std::unique_lock<std::mutex> lock(replication_latch_);
        bool predicate = replication_cv_.wait_for(lock, replication_timeout_,
                                                  [&] { return !replication_active_ || !arrived_buffer_queue_.empty(); });
        // If we timeout or replication is shut down, return false
        if (!predicate || !replication_active_) return false;

        TERRIER_ASSERT(!arrived_buffer_queue_.empty(),
                       "If we did not shut down or timeout, CV should only wake up when a new buffer arrives");
        curr_buffer_ = std::move(arrived_buffer_queue_.front());
        arrived_buffer_queue_.pop();
      }

      // Read in as much as as is availible in thus buffer.
      auto readable_size = curr_buffer_->HasMore(size) ? size : curr_buffer_->BytesAvailable();
      curr_buffer_->ReadIntoView(readable_size).Read(readable_size, dest);

      // If we didn't read all that we needed to, call Read again to read the rest of the data
      // We cast dest to char* to increase pointer by the size we read.
      return (readable_size < size) ? Read(static_cast<char *>(dest) + readable_size, size - readable_size) : true;
    }
  };

}  // namespace terrier::storage
=======
class ReplicationLogProvider {
 public:
  /**
   * Passes the content of the buffer to traffic cop
   * @param buffer content to pass to traffic cop
   */
  virtual void HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer);
};
}  // namespace noisepage::storage
>>>>>>> 193244ce13033c9e65563c6e7d0ccdedd0eaf973
