#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::replication {

class ReplicationManager;

/** Abstraction around a replica. */
class Replica {
 public:
  Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
          const std::string &hostname, int port);

  common::ManagedPointer<messenger::ConnectionId> GetConnectionId() { return common::ManagedPointer(&connection_); }

 private:
  friend ReplicationManager;

  messenger::ConnectionDestination replica_info_;
  messenger::ConnectionId connection_;
};

/**
 * ReplicationManager is responsible for all aspects of database replication.
 *
 * Housekeeping duties include:
 * - Maintaining a list of all known database replicas and the presumed state of each replica.
 * - Sending periodic heartbeats to other database replicas.
 *
 * The ReplicationManager can be invoked by other components in order to send specific replication-related data.
 * For example, the LogManager may ask the ReplicationManager to replicate a specific log record to all other replicas.
 *
 * All replication communication happens over REPLICATION_DEFAULT_PORT.
 */
class ReplicationManager {
 public:
  enum class MessageType : uint8_t { RESERVED = 0, HEARTBEAT };
  static constexpr int REPLICATION_DEFAULT_PORT = 15445;

  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity);

  void ReplicaConnect(const std::string &replica_name, const std::string &hostname, int port);

  void ReplicaSend(const std::string &replica_name, const MessageType type, const std::string &msg, bool block);

 private:
  void EventLoop(common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg);

  void ReplicaHeartbeat(const std::string &replica_name);

  common::ManagedPointer<messenger::ConnectionId> GetReplica(const std::string &replica_name);

  common::ManagedPointer<messenger::Messenger> messenger_;
  std::unordered_map<std::string, Replica> replicas_;  //< Replica Name -> Connection ID
  std::mutex mutex_;
  std::condition_variable cvar_;
};

}  // namespace noisepage::replication