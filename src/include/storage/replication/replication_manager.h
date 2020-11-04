#pragma once

#include <fstream>
#include "common/container/concurrent_queue.h"
#include "storage/recovery/replication_log_provider.h"
#include "network/network_io_utils.h"
#include "messenger/messenger.h"
#include "messenger/connection_destination.h"
#include "common/json.h"
#include "loggers/storage_logger.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace noisepage::storage {

class ReplicationManager {
public:

  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger, common::ManagedPointer<storage::ReplicationLogProvider> provider) : messenger_(messenger), provider_(provider) {
  }

  // Adds a record buffer to the current queue.
  void AddRecordBuffer(BufferedLogWriter *network_buffer);

  // Serialize log record buffer to json and send the message across the network.
  bool SendMessage(messenger::ConnectionId &target);

  // Parse the log record buffer and redirect to replication log provider for recovery.
  void Recover(const std::string& string_view);

private:
  std::string master_address_;
  std::string replica_address_;
  common::ManagedPointer<messenger::Messenger> messenger_;
  common::ManagedPointer<storage::ReplicationLogProvider> provider_;

  // Keeps track of stored record buffers.
  common::ConcurrentQueue<SerializedLogs> replication_consumer_queue_;
};

}  // namespace noisepage::storage;