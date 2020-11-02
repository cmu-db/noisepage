#include "replication/replication_manager.h"

#include "loggers/replication_logger.h"

noisepage::replication::Replica::Replica(noisepage::common::ManagedPointer<noisepage::messenger::Messenger> messenger,
                                         const std::string &replica_name, const std::string &hostname, int port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, port)),
      connection_(messenger->MakeConnection(replica_info_)) {}

noisepage::replication::ReplicationManager::ReplicationManager(
    noisepage::common::ManagedPointer<noisepage::messenger::Messenger> messenger, const std::string &network_identity)
    : messenger_(messenger) {
  auto listen_destination = messenger::ConnectionDestination::MakeTCP("", "127.0.0.1", REPLICATION_DEFAULT_PORT);
  messenger_->ListenForConnection(listen_destination, network_identity,
                                  [this](common::ManagedPointer<messenger::Messenger> messenger,
                                         const messenger::ZmqMessage &msg) { EventLoop(messenger, msg); });
}

void noisepage::replication::ReplicationManager::ReplicaConnect(const std::string &replica_name,
                                                                const std::string &hostname, int port) {
  replicas_.try_emplace(replica_name, messenger_, replica_name, hostname, port);
}

void noisepage::replication::ReplicationManager::ReplicaSend(
    const std::string &replica_name, const noisepage::replication::ReplicationManager::MessageType type,
    const std::string &msg, bool block) {
  std::unique_lock<std::mutex> lock(mutex_);
  bool completed = false;
  messenger_->SendMessage(
      GetReplica(replica_name), msg,
      [this, &completed](common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg) {
        completed = true;
        cvar_.notify_all();
      },
      static_cast<uint64_t>(type));

  if (block) {
    // If the caller requested to block until the operation was completed, the thread waits.
    cvar_.wait(lock, [&completed] { return completed; });
  }
  lock.unlock();
}

void noisepage::replication::ReplicationManager::EventLoop(
    noisepage::common::ManagedPointer<noisepage::messenger::Messenger> messenger,
    const noisepage::messenger::ZmqMessage &msg) {
  switch (static_cast<MessageType>(msg.GetCallbackIdReceiver())) {
    case MessageType::HEARTBEAT:
      REPLICATION_LOG_TRACE(fmt::format("Heartbeat from: {}", msg.GetRoutingId()));
      break;
    default:
      break;
  }
}

void noisepage::replication::ReplicationManager::ReplicaHeartbeat(const std::string &replica_name) {
  messenger_->SendMessage(GetReplica(replica_name), "", messenger::CallbackFns::Noop,
                          static_cast<uint64_t>(MessageType::HEARTBEAT));
}

noisepage::common::ManagedPointer<noisepage::messenger::ConnectionId>

noisepage::replication::ReplicationManager::GetReplica(const std::string &replica_name) {
  return replicas_.at(replica_name).GetConnectionId();
}
