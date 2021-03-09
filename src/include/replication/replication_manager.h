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
class PrimaryReplicationManager;
class ReplicaReplicationManager;

// TODO(WAN): I am currently not sure what methods will be specific to communicating with a replica.
//  If you see logic in ReplicationManager that can be pushed into Replica please let me know.
/** Abstraction around a replica. */
class Replica {
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

  messenger::ConnectionDestination replica_info_;  ///< The connection metadata for this replica.
  messenger::ConnectionId connection_;             ///< The connection to this replica.
  uint64_t last_heartbeat_;  ///< Time (unix epoch) that the replica heartbeat was last successful.
};

/** Container class for the contents of a REPLICATE_BUFFER message. */
class ReplicateBufferMessage {
 public:
  static const char *key_buf_id;   ///< JSON key for buffer ID. Helps to order buffers.
  static const char *key_content;  ///< JSON key for buffer content. The actual buffer itself.

  /** Construct a new buffer message (sender side). */
  ReplicateBufferMessage(uint64_t buffer_id, std::string &&contents)
      : buffer_id_(buffer_id), contents_(std::move(contents)) {}

  /** @return The parsed ReplicateBufferMessage. */
  static ReplicateBufferMessage FromMessage(const messenger::ZmqMessage &msg);

  /** @return The JSON version of a ReplicateBufferMessage, ready for ReplicaSend(). */
  common::json ToJson();

  /** @return Only valid on receiver side. The source callback ID to invoke once this buffer is considered persisted. */
  uint64_t GetSourceCallbackId() const { return source_callback_id_; }

  /** @return The ID of this buffer. */
  uint64_t GetMessageId() const { return buffer_id_; }

  /** @return The contents of this buffer. */
  const std::string &GetContents() const { return contents_; }

 private:
  /** Construct a new buffer message (receiver side). */
  ReplicateBufferMessage(uint64_t buffer_id, std::string &&contents, uint64_t source_callback_id)
      : buffer_id_(buffer_id), contents_(std::move(contents)), source_callback_id_(source_callback_id) {}

  uint64_t buffer_id_;    ///< The buffer ID identifies the order of buffers sent by the remote origin.
  std::string contents_;  ///< The actual contents of the buffer.
  /**
   * The source callback ID that should be invoked once this buffer is persisted.
   * Since this is added by the messenger, this is just left uninitialized on the sender side.
   */
  uint64_t source_callback_id_;
};

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
  /** The type of message that is being sent. */
  enum class MessageType : uint8_t {
    RESERVED = 0,     ///< Reserved.
    ACK,              ///< Acknowledgement of received message.
    HEARTBEAT,        ///< Replica heartbeat.
    REPLICATE_BUFFER  ///< Buffer received from the log manager that should be replicated.
  };
  /** Milliseconds between replication heartbeats before a replica is declared dead. */
  static constexpr uint64_t REPLICATION_CARDIAC_ARREST_MS = 5000;
  /** Maximum wait time for synchronous replication. */
  static constexpr std::chrono::seconds REPLICATION_MAX_BLOCKING_WAIT_TIME = std::chrono::seconds(10);

  static const char *key_message_type;  ///< JSON key for the message type.

  /** Default destructor. */
  virtual ~ReplicationManager();

  /** Enable replication. */
  void EnableReplication();

  /** Disable replication. */
  void DisableReplication();

  /** @return The port that replication is running on. */
  uint16_t GetPort() const { return port_; }

  /**
   * @return    On the primary, returns the last record ID that was successfully transmitted to all replicas.
   *            On a replica, returns the last record ID that was successfully received.
   */
  virtual uint64_t GetLastRecordId() const = 0;

  /** @return   True if this is the primary node and false if this is a replica. */
  virtual bool IsPrimary() const = 0;

  /** @return   True if this is a replica node and false if this is the primary. */
  bool IsReplica() const { return !IsPrimary(); }

  /** @return   This should only be called from the primary node! Return a pointer as the primary recovery manager. */
  common::ManagedPointer<PrimaryReplicationManager> GetAsPrimary();

  /** @return   This should only be called from a replica node! Return a pointer as a replica recovery manager. */
  common::ManagedPointer<ReplicaReplicationManager> GetAsReplica();

  /**
   * Send an acknowledgement message to the specified replica.
   *
   * @param replica_name    The replica to send to.
   * @param callback_id     The ID of the callback to invoke on the node.
   * @param block           True if the call should block until an acknowledgement is received. False otherwise.
   */
  void ReplicaAck(const std::string &replica_name, uint64_t callback_id, bool block);

  /**
   * Send a message to the specified replica.
   *
   * @param replica_name    The replica to send to.
   * @param msg             The message that is being sent.
   * @param block           True if the call should block until an acknowledgement is received. False otherwise.
   */
  void ReplicaSend(const std::string &replica_name, common::json msg, bool block);

 protected:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connect to all the
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

  /** The main event loop that all nodes run. This handles receiving messages. */
  virtual void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg);

  /**
   * Request a heartbeat from the specified replica.
   * Note that this function does not wait for a response and will update the replica's last heartbeat time in the
   * background. This means that ReplicaHeartbeat may need to be invoked again to realize that the replica is dead.
   *
   * If REPLICATION_CARDIAC_ARREST_MS seconds have passed since the last acknowledgement, the replica is marked dead.
   *
   * @param replica_name                The replica to request a heartbeat from.
   */
  void ReplicaHeartbeat(const std::string &replica_name);

  /**
   * Build the list of replicas as specified in replication.config.
   *
   * @param replication_hosts_path      The path to the replication.config file.
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

  /** @return The connection ID associated with a particular replica. */
  common::ManagedPointer<messenger::ConnectionId> GetReplicaConnection(const std::string &replica_name);

  static constexpr size_t MESSAGE_PREVIEW_LEN = 20;  ///< The number of characters to preview in a message in debug.

  common::ManagedPointer<messenger::Messenger> messenger_;  ///< The messenger used for all send/receive operations.
  std::string identity_;                                    ///< The identity of this replica.
  uint16_t port_;                                           ///< The port that replication runs on.
  std::unordered_map<std::string, Replica> replicas_;       ///< Replica Name -> Connection ID.

  std::mutex blocking_send_mutex_;              ///< Mutex used for blocking sends.
  std::condition_variable blocking_send_cvar_;  ///< Cvar used for blocking sends.

  // TODO(WAN): I think it is better to use retention policies instead of enabling/disabling replication.
  bool replication_enabled_ = false;  ///< True if replication is currently enabled and false otherwise.

  /** Once used, buffers are returned to a central empty buffer queue. */
  common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue_;

 private:
  /**
   * Send a message to the specified replica.
   *
   * @param replica_name    The replica to send to.
   * @param msg_type        The type of message that is being sent.
   * @param msg             The message that is being sent.
   * @param remote_cb_id    The ID of the callback to be invoked on the remote node.
   * @param block           True if the call should block until an acknowledgement is received. False otherwise.
   */
  void ReplicaSendInternal(const std::string &replica_name, MessageType msg_type, common::json msg,
                           uint64_t remote_cb_id, bool block);
};

/** The replication manager that should run on the primary. */
class PrimaryReplicationManager final : public ReplicationManager {
 public:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connect to all the
   * replicas specified in the replication.config file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.config file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  PrimaryReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /** Destructor. */
  ~PrimaryReplicationManager() final;

  /** @return True since this is the primary. */
  bool IsPrimary() const override { return true; }

  /** @return The last record ID that was successfully transmitted to all replicas. */
  uint64_t GetLastRecordId() const override { return next_buffer_sent_id_ - 1; }

  /**
   * Send a buffer to all the replicas as a REPLICATE_BUFFER message.
   *
   * @param buffer          The buffer to be replicated.
   */
  void ReplicateBuffer(storage::BufferedLogWriter *buffer);

 protected:
  uint64_t next_buffer_sent_id_ = 1;  ///< The ID of the next buffer to send.
};

/**
 * The replication manager that should run on the replicas.
 *
 * TODO(WAN): If the primary fails, how does the replica switch its replication manager? I guess possible, but annoying.
 */
class ReplicaReplicationManager final : public ReplicationManager {
 public:
  /**
   * On construction, the replication manager establishes a listen destination on the messenger and connect to all the
   * replicas specified in the replication.config file located at @p replication_hosts_path.
   *
   * @param messenger                   The messenger instance to use.
   * @param network_identity            The identity of this node in the network.
   * @param port                        The port to listen on.
   * @param replication_hosts_path      The path to the replication.config file.
   * @param empty_buffer_queue          A queue of empty buffers that the replication manager may return buffers to.
   */
  ReplicaReplicationManager(
      common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
      const std::string &replication_hosts_path,
      common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue);

  /** Destructor. */
  ~ReplicaReplicationManager() final;

  /** @return False since this is a replica. */
  bool IsPrimary() const override { return false; }

  /** @return The last record ID that was successfully received. */
  uint64_t GetLastRecordId() const override { return last_record_received_id_; }

  /** @return The replication log provider that (ordered) replication logs are pushed to. */
  common::ManagedPointer<storage::ReplicationLogProvider> GetReplicationLogProvider() const {
    return common::ManagedPointer(provider_);
  }

 protected:
  /** The main event loop that all nodes run. This handles receiving messages. */
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg) override;

  /** Apply the buffer if it is the "next" buffer. Otherwise enqueue it, see received_message_queue_ documentation. */
  void HandleReplicatedBuffer(const messenger::ZmqMessage &msg);

  std::unique_ptr<storage::ReplicationLogProvider> provider_;  ///< The replicated buffers provided by the primary.
  uint64_t last_record_received_id_ = 0;                       ///< The ID of the last record to be received.

  /**
   * The received messages are queued up until the "next" received message is the right one, i.e.,
   * the received message has buf_id that is equal to last_record_received_id_ + 1.
   * Essentially, this is a local buffer that helps to provide the illusion that all messages received in order.
   * When the right buffer is received, the buffers are repeatedly removed and forwarded to the provider_
   * until there is again a "gap" in messages received.
   */
  std::priority_queue<ReplicateBufferMessage, std::vector<ReplicateBufferMessage>,
                      std::function<bool(ReplicateBufferMessage, ReplicateBufferMessage)>>
      received_message_queue_;
};

}  // namespace noisepage::replication
