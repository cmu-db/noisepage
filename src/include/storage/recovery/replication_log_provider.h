#pragma once

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

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
  /**
   * Create a new ReplicationLogProvider.
   *
   * @param replication_timeout         The replication timeout. TODO(WAN): kill this?
   */
  explicit ReplicationLogProvider(std::chrono::seconds replication_timeout)
      : replication_active_(true), replication_timeout_(replication_timeout) {}

  LogProviderType GetType() const override { return LogProviderType::REPLICATION; }

  /**
   * Notifies the log provider that replication is ending.
   */
  void EndReplication() {
    std::unique_lock<std::mutex> lock(replication_latch_);
    replication_active_ = false;
    replication_cv_.notify_all();
  }

  /** Buffer the message. */
  void AddBufferFromMessage(const uint64_t primary_callback_id, const std::string &content) {
    std::vector<unsigned char> bytes(content.begin(), content.end());
    network::ReadBufferView view(bytes.size(), bytes.begin());
    auto buffer = std::make_unique<network::ReadBuffer>();
    buffer->FillBufferFrom(view, bytes.size());
    {
      std::unique_lock<std::mutex> lock(replication_latch_);
      arrived_buffer_queue_.emplace(primary_callback_id, std::move(buffer));
      replication_cv_.notify_one();
    }

    // TODO(WAN): if you want sync/async I think this is where you do it
  }

  /** @return True if there are more records. False otherwise. */
  bool NonBlockingHasMoreRecords() const {
    return (curr_buffer_ != nullptr && curr_buffer_->HasMore()) || !arrived_buffer_queue_.empty();
  }

  /** Unlock the primary ackables. */
  void LatchPrimaryAckables() { replication_latch_.lock(); }
  /** Unlock the primary ackables. */
  void UnlatchPrimaryAckables() { replication_latch_.unlock(); }
  /** @return A reference to the vector of source callback IDs from the primary that should be ack'd. */
  std::vector<uint64_t> &GetPrimaryAckables() { return primary_ackables_; }

 private:
  /** True if replication is currently active. */
  bool replication_active_;

  // TODO(Gus): Put in settings manager
  /** The number of seconds before replication times out. */
  std::chrono::seconds replication_timeout_;

  // Current buffer to read logs from
  std::unique_ptr<network::ReadBuffer> curr_buffer_ = nullptr;

  // (Primary callback ID, buffers) that have arrived in packets from the master node
  // TODO(Gus): For now, these buffers will most likely be allocated on demand by the network layer. We could consider
  // having a buffer pool if allocation becomes a bottleneck
  std::queue<std::pair<uint64_t, std::unique_ptr<network::ReadBuffer>>> arrived_buffer_queue_;

  /**
   * Callbacks that should be invoked on the primary once the current set of buffers that have been taken off the
   * arrived buffer queue have been processed.
   */
  std::vector<uint64_t> primary_ackables_;

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
    //  2. Someone ends replication
    //  3. We have a current buffer and it has more bytes available
    //  4. A new packet has arrived
    replication_cv_.wait(lock, [&] { return !replication_active_ || NonBlockingHasMoreRecords(); });
    return replication_active_;
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
      auto &buffer_elem = arrived_buffer_queue_.front();
      // The first element is the numeric callback that we should invoke on the primary after processing buffers.
      primary_ackables_.emplace_back(buffer_elem.first);
      curr_buffer_ = std::move(buffer_elem.second);
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
