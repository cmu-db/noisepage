#pragma once

#include <fstream>
#include "common/container/concurrent_queue.h"
#include "common/json.h"
#include "loggers/storage_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"
#include "network/network_io_utils.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace noisepage::storage {

/** Replication manager is reponsible for information exchange between primary and replicas. */
class ReplicationManager {
 public:
  /**
   * Create a Replication Manager that uses the given messenger to exchange information and the given log provider for
   * recovery.
   * @param messenger   The messenger that will be used to exchange information.
   * @param provider    The replication log provider used for recovery.
   */
  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger,
                     common::ManagedPointer<storage::ReplicationLogProvider> provider)
      : messenger_(messenger), provider_(provider) {}

  /**
   * Adds a record buffer to the current queue.
   * @param network_buffer The buffer to be added to the queue.
   */
  void AddLogRecordBuffer(BufferedLogWriter *network_buffer);

  /**
   * Serialize log record buffer to json and send the message across the network. This operation empties the record
   * buffer queue.
   * @param target Target to send the message to.
   * @param msg_id Message id.
   */
  bool SendSerializedLogRecords(messenger::ConnectionId &target, uint8_t msg_id);

  /** Parse the log record buffer and redirect to replication log provider for recovery. */
  void RecoverFromSerializedLogRecords(const std::string &string_view);

 private:
  common::ManagedPointer<messenger::Messenger> messenger_;
  common::ManagedPointer<storage::ReplicationLogProvider> provider_;

  /** Keeps track of currently stored record buffers. */
  common::ConcurrentQueue<SerializedLogs> replication_consumer_queue_;

  /** Used for determining whether the message being sent over is used for replication. */
  const std::string replication_message_identifier = "replication";
};

}  // namespace noisepage::storage