#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/container/concurrent_queue.h"
#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"
#include "network/network_io_utils.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace noisepage::replication {

class ReplicationManager;

/** Abstraction around a replica. */
class Replica {
  // todo(wan): justify existence? what other methods are specific to communicating with a replica?
 public:
  Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
          const std::string &hostname, uint16_t port);

  common::ManagedPointer<messenger::ConnectionId> GetConnectionId() { return common::ManagedPointer(&connection_); }

 private:
  friend ReplicationManager;

  messenger::ConnectionDestination replica_info_;
  messenger::ConnectionId connection_;
  uint64_t last_heartbeat_;  //< Time (unix epoch) that the replica heartbeat was last successful.
};

/**
 * ReplicationManager is responsible for all aspects of database replication.
 *
 * Housekeeping duties include:
 * - Maintaining a list of all known database replicas and the presumed state of each replica.
 * - Sending periodic heartbeats to other database replicas.
 *
 * The ReplicationManager can be invoked by other components in order to send specific replication-related data.
 * For example:
 * - LogManager may ask the ReplicationManager to replicate a specific log record to all other replicas.
 * - Catalog may ask the ReplicationManager to replicate some specific in-memory data.
 *
 * This is coordinated through MessageType below. The ReplicationManager is aware of all required functionality from
 * the subsystems that the ReplicationManager interfaces with. The ReplicationManager is responsible for invoking
 * the appropriate functions.
 *
 * (this logic is pushed to the ReplicationManager to avoid littering the rest of the codebase with retry/failure etc)
 *
 * All replication communication happens over REPLICATION_DEFAULT_PORT.
 */
class ReplicationManager {
 public:
  enum class MessageType : uint8_t { HEARTBEAT = 0, RECOVER = 1, ACK = 2 };
  /** Milliseconds between replication heartbeats before a replica is declared dead. */
  static constexpr uint64_t REPLICATION_CARDIAC_ARREST_MS = 5000;

  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity,
                     uint16_t port, const std::string &replication_hosts_path,
                     common::ManagedPointer<storage::ReplicationLogProvider> provider);

  /**
   * Establish a connection to a replica.
   * @param replica_name replica to connect to.
   * @param hostname replica hostname.
   * @param port replica port.
   */
  void ReplicaConnect(const std::string &replica_name, const std::string &hostname, uint16_t port);

  /**
   * Sends a replication message to the replica.
   *
   * TODO(wan/tianlei): sync/async seems relatively easy to do in this framework, though I need to fix up the
   * multithreaded block case just capture a bool in the callback and flip it on response, have the cvar wait on that I
   * don't think this should actually send the message, though - it should buffer the message to be sent instead
   *
   * @param replica_name The replica to send the replication command to.
   * @param type Type of replication message.
   * @param msg The message to be sent (e.g. serialized log records).
   * @param block synchronous/asynchronous
   */
  void ReplicaSend(const std::string &replica_name, MessageType type, const std::string &msg, bool block);

  void ReplicaTestSend(const std::string&msg, bool block) {
  }

  /**
   * Sends an acknowledgement to the replica that the replication command is carried out successfully.
   * @param replica_name replica to send the replication command to.
   */
  void ReplicaAck(const std::string &replica_name);

  /** @return The port that replication is running on. */
  uint16_t GetPort() const { return port_; }

  std::vector<std::string> GetCurrentReplicaList() const {
    std::vector<std::string> replicas;
    replicas.reserve(replicas_.size());
    for (const auto &replica : replicas_) {
      replicas.emplace_back(replica.first);
    }
    return replicas;
  }

  /**
   * Adds a record buffer to the current queue.
   * @param network_buffer The buffer to be added to the queue.
   */
  void AddLogRecordBuffer(storage::BufferedLogWriter *network_buffer);

  /**
   * Serialize log record buffer to json. This operation empties the record
   * buffer queue.
   * @return serialized json string
   */
  std::string SerializeLogRecords();

  /** Parse the log record buffer and redirect to replication log provider for recovery. */
  void RecoverFromSerializedLogRecords(const std::string &string_view);

  void SetRecoveryManager(common::ManagedPointer<storage::RecoveryManager> recovery_manager) {
    recovery_manager_ = recovery_manager;
  }

  void StartRecovery() { recovery_manager_->StartRecovery(); }

  common::ManagedPointer<storage::RecoveryManager> GetRecoveryManager() { return recovery_manager_; }

  uint64_t ReplicaSize() { return replicas_.size(); }

 private:
  void EventLoop(common::ManagedPointer<noisepage::messenger::Messenger> messenger, std::string_view &sender_id,
                 std::string_view &msg, uint64_t recv_cb_id);
  void ReplicaHeartbeat(const std::string &replica_name);

  void BuildReplicaList(const std::string &replication_hosts_path);

  // TODO(wan/tianlei): division of functionality?'
  // TODO(wan/tianlei): RM would become a dependency to most components

  common::ManagedPointer<messenger::ConnectionId> GetReplicaConnection(const std::string &replica_name);

  common::ManagedPointer<messenger::Messenger> messenger_;
  std::string identity_;                               //< The identity of this replica.
  uint16_t port_;                                      //< The port that replication runs on.
  std::unordered_map<std::string, Replica> replicas_;  //< Replica Name -> Connection ID.
  std::mutex mutex_;
  std::condition_variable cvar_;

  common::ManagedPointer<storage::RecoveryManager> recovery_manager_;
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
  /** Keeps track of currently stored record buffers. */
  common::ConcurrentQueue<storage::SerializedLogs> replication_consumer_queue_;
  /** Used for determining whether the message being sent over is used for replication. */
  const std::string replication_message_identifier_ = "re_";
  const std::string ack_message_identifier_ = "ac_";
  const std::string heartbeat_message_identifier_ = "hb_";
};

}  // namespace noisepage::replication