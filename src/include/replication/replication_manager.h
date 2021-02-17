#pragma once

#include <condition_variable>  // NOLINT
#include <memory>
#include <mutex>  // NOLINT
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "common/json_header.h"
#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::storage {
class AbstractLogProvider;
class BufferedLogWriter;
class ReplicationLogProvider;
}  // namespace noisepage::storage

namespace noisepage::replication {

class ReplicationManager;

/** Abstraction around a replica. */
class Replica {
  // todo(wan): justify existence? what other methods are specific to communicating with a replica?
 public:
  /**
   * Create a replica.
   *
   * @param messenger       The messenger to use.
   * @param replica_name    The name of the replica.
   * @param hostname        The hostname of the replica.
   * @param port            The port of the replica.
   */
  Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
          const std::string &hostname, uint16_t port);

  /** @return The connection ID for this replica. */
  common::ManagedPointer<messenger::ConnectionId> GetConnectionId() { return common::ManagedPointer(&connection_); }

 private:
  friend ReplicationManager;

  messenger::ConnectionDestination replica_info_;
  messenger::ConnectionId connection_;
  uint64_t last_heartbeat_;  ///< Time (unix epoch) that the replica heartbeat was last successful.
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
  /** The type of message that is being sent. */
  enum class MessageType : uint8_t { RESERVED = 0, ACK, HEARTBEAT, REPLICATE_BUFFER };
  /** Milliseconds between replication heartbeats before a replica is declared dead. */
  static constexpr uint64_t REPLICATION_CARDIAC_ARREST_MS = 5000;

  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connect to all the
   * replicas specified in the replication.conf file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.conf file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  ReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /** Default destructor. */
  ~ReplicationManager();

  /**
   * Send a message to the specified replica.
   *
   * @param replica_name    The replica to send to.
   * @param type            The type of message that is being sent.
   * @param msg             The message that is being sent.
   * @param block           True if the call should block until an acknowledgement is received. False otherwise.
   */
  void ReplicaSend(const std::string &replica_name, MessageType type, const std::string &msg, bool block);

  /**
   * Send a buffer to all the replicas as a REPLICATE_BUFFER message.
   *
   * @param buffer          The buffer to be replicated.
   */
  void ReplicateBuffer(storage::BufferedLogWriter *buffer);

  /** @return The port that replication is running on. */
  uint16_t GetPort() const { return port_; }

  /** @return The replication log provider that (ordered) replication logs are pushed to. */
  common::ManagedPointer<storage::ReplicationLogProvider> GetReplicationLogProvider() const {
    return common::ManagedPointer(provider_);
  }

  /**
   * @return    On the primary, returns the last record ID that was successfully transmitted to all replicas.
   *            On a replica, returns the last record ID that was successfully applied.
   */
  uint64_t GetLastRecordId() const { return IsPrimary() ? next_buffer_sent_id_ - 1 : last_record_applied_id_; }

  /** Enable replication. */
  void EnableReplication() { replication_enabled_ = true; }

  /** Disable replication. */
  void DisableReplication() { replication_enabled_ = false; }

  /** @return   True if this is the primary node and false if this is a replica. */
  bool IsPrimary() const { return identity_ == "primary"; }

 private:
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg);
  void ReplicaHeartbeat(const std::string &replica_name);

  /**
   * Build the list of replicas as specified in replication.conf.
   *
   * @param replication_hosts_path      The path to the replication_hosts.conf file.
   */
  void BuildReplicaList(const std::string &replication_hosts_path);

  /**
   * Connect to the specified replica.
   *
   * @param replica_name                The name to assign the replica.
   * @param hostname                    The hostname where the replica is available.
   * @param port                        The port where the replica is available.
   */
  void ReplicaConnect(const std::string &replica_name, const std::string &hostname, uint16_t port);

  common::ManagedPointer<messenger::ConnectionId> GetReplicaConnection(const std::string &replica_name);

  common::ManagedPointer<messenger::Messenger> messenger_;  ///< The messenger used for all send/receive operations.
  std::string identity_;                                    ///< The identity of this replica.
  uint16_t port_;                                           ///< The port that replication runs on.
  std::unordered_map<std::string, Replica> replicas_;       ///< Replica Name -> Connection ID.

  std::unique_ptr<storage::ReplicationLogProvider> provider_;  ///< The replicated buffers provided by the primary.
  std::mutex blocking_send_mutex_;                             ///< Mutex used for blocking sends.
  std::condition_variable blocking_send_cvar_;                 ///< Cvar used for blocking sends.

  // TODO(WAN): I believe this can be removed with Tianlei's PR
  bool replication_enabled_ = false;  ///< True if replication is currently enabled and false otherwise.

  uint64_t next_buffer_sent_id_ = 1;     ///< The ID of the next buffer to sent.
  uint64_t last_record_applied_id_ = 0;  ///< The ID of the last record to be applied.

  /** The received buffers are queued up until the "next" received buffer is the right one. */
  std::priority_queue<nlohmann::json, std::vector<nlohmann::json>, std::function<bool(nlohmann::json, nlohmann::json)>>
      received_buffer_queue_;

  /** Once used, buffers are returned to a central empty buffer queue. */
  common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue_;
};

}  // namespace noisepage::replication
