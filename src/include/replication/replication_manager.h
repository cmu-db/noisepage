#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/container/concurrent_blocking_queue.h"
#include "common/managed_pointer.h"
#include "replication/replica.h"
#include "replication/replication_messages.h"

namespace noisepage::storage {
class BufferedLogWriter;
}  // namespace noisepage::storage

namespace noisepage::replication {

class PrimaryReplicationManager;
class ReplicaReplicationManager;

/**
 * ReplicationManager is responsible for all aspects of database replication.
 *
 * Housekeeping duties include:
 * - Maintaining a list of all known database replicas and the presumed state of each replica.
 * - Sending periodic heartbeats to other database replicas. TODO(WAN): Heartbeat works, but is unused.
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
 * Code is mainly pushed into the ReplicationManager to avoid other system components needing retry/failure logic.
 *
 * All replication communication happens over REPLICATION_DEFAULT_PORT.
 */
class ReplicationManager {
 public:
  /** Default destructor. */
  virtual ~ReplicationManager();

  /** @return   The port that replication is running on. */
  uint16_t GetPort() const { return port_; }
  /** @return   True if this is the primary node and false if this is a replica. */
  virtual bool IsPrimary() const = 0;
  /** @return   True if this is a replica node and false if this is the primary. */
  bool IsReplica() const { return !IsPrimary(); }
  /** @return   This should only be called from the primary node! Return a pointer as the primary recovery manager. */
  common::ManagedPointer<PrimaryReplicationManager> GetAsPrimary();
  /** @return   This should only be called from a replica node! Return a pointer as a replica recovery manager. */
  common::ManagedPointer<ReplicaReplicationManager> GetAsReplica();

 protected:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connects to all the
   * replicas specified in the replication.config file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.config file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  ReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /**
   * @return    The ID of the next message which should be used.
   * @warning   All ReplicationManager methods, including derived classes, must use this for message IDs.
   */
  msg_id_t GetNextMessageId();

  /**
   * Send the message to the given destination.
   * @param destination                 The destination to send the message to.
   * @param msg_id                      The ID of the message.
   * @param message                     The message to send.
   * @param source_callback             The callback to invoke on the response received, can be nullptr.
   * @param destination_callback        The callback that should be invoked on the destination.
   */
  void Send(const std::string &destination, msg_id_t msg_id, const std::string &message,
            const messenger::CallbackFn &source_callback, messenger::callback_id_t destination_callback);

  /** The main event loop that all nodes run. This handles receiving messages. */
  virtual void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &zmq_msg,
                         common::ManagedPointer<BaseReplicationMessage> msg);

  static constexpr size_t MESSAGE_PREVIEW_LEN = 100;   ///< The number of characters to preview in a message in debug.
  std::unordered_map<std::string, Replica> replicas_;  ///< Replica Name -> Connection ID.
  /** Once used, buffers are returned to a central empty buffer queue. */
  common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue_;

 private:
  /**
   * Build the replication network topology as specified in replication.config.
   *
   * @param replication_hosts_path      The path to the replication.config file.
   */
  void BuildReplicationNetwork(const std::string &replication_hosts_path);

  /**
   * Connect to the specified node.
   *
   * @param node_name                   The name to assign the node.
   * @param hostname                    The hostname where the node is available.
   * @param port                        The port where the node is available.
   */
  void NodeConnect(const std::string &node_name, const std::string &hostname, uint16_t port);

  /** @return The connection ID associated with a particular replica. */
  messenger::connection_id_t GetNodeConnection(const std::string &replica_name);

  common::ManagedPointer<messenger::Messenger> messenger_;  ///< The messenger used for all send/receive operations.
  std::string identity_;                                    ///< The identity of this replica.
  uint16_t port_;                                           ///< The port that replication runs on.

  std::atomic<msg_id_t> next_msg_id_{1};  ///< ID of the next message being sent out.
};

}  // namespace noisepage::replication
