#pragma once

#include <utility>
#include <vector>
#include <string>
#include <memory>

#include "network/network_io_utils.h"
#include "network/network_io_wrapper.h"
#include "storage/write_ahead_log/log_consumer_task.h"

namespace terrier::storage {

/**
 * A ReplicationLogConsumerTask is responsible for writing serialized log records over network to any replicas listening
 */
class ReplicationLogConsumerTask final : public LogConsumerTask {
 public:
  // TODO(Gus): Take in IP addresses as vector to support multiple replicas
  /**
   * Constructs a new DiskLogConsumerTask
   * @param persist_interval Interval time for when to persist log file
   * @param persist_threshold threshold of data written since the last persist to trigger another persist
   */
  explicit ReplicationLogConsumerTask(const std::string &ip_address, uint16_t port,
                                      common::ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue,
                                      common::ConcurrentQueue<storage::SerializedLogs> *filled_buffer_queue)
      : LogConsumerTask(empty_buffer_queue, filled_buffer_queue) {
    io_wrapper_ = std::make_unique<network::NetworkIoWrapper>(ip_address, port);
  }

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override;

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override;

 private:
  friend class LogManager;
  std::unique_ptr<network::NetworkIoWrapper> io_wrapper_;

  // Unique, monotonically increasing identifier for each message sent
  uint64_t message_id_ = 0;

  // Synchronisation primitives to synchronize sending logs over network
  std::mutex replication_lock_;
  // Condition variable to signal replication task thread to wake up and send logs over network or if shutdown has
  // initiated, then quit
  std::condition_variable replication_log_sender_cv_;

  /**
   * @brief Main task loop.
   * Loops until task is shut down. In the case of shutdown, guarantees sending of all serialized records at the time of
   * shutdown
   */
  void ReplicationLogConsumerTaskLoop();

  /**
   * @brief Sends all serialized logs to replica(s)
   */
  void SendLogsOverNetwork();

  /**
   * Sends a stop message to replica(s)
   */
  void SendStopReplicationMessage();
};
}  // namespace terrier::storage
