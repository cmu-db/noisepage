#include "replication/replication_manager.h"

#include <fstream>

#include "common/error/exception.h"
#include "loggers/replication_logger.h"

namespace noisepage::replication {

ReplicationManager::ReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : empty_buffer_queue_(empty_buffer_queue), messenger_(messenger), identity_(network_identity), port_(port) {
  replication_logger->set_level(spdlog::level::trace);
  // Start listening on the given replication port.
  messenger::ConnectionDestination listen_destination =
      messenger::ConnectionDestination::MakeTCP("", "127.0.0.1", port);
  messenger_->ListenForConnection(
      listen_destination, network_identity,
      [this](common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg) {
        auto json = nlohmann::json::parse(msg.GetMessage());
        auto replication_msg = BaseReplicationMessage::ParseFromJson(json);
        EventLoop(messenger, msg, common::ManagedPointer(replication_msg));
      });
  // Connect to all of the other nodes.
  BuildReplicationNetwork(replication_hosts_path);
}

ReplicationManager::~ReplicationManager() = default;

msg_id_t ReplicationManager::GetNextMessageId() { return next_msg_id_++; }

void ReplicationManager::SendAckForMessage(const messenger::ZmqMessage &zmq_msg, const BaseReplicationMessage &msg) {
  REPLICATION_LOG_TRACE(fmt::format("[SEND] AckMsg -> {}: {}", zmq_msg.GetRoutingId(), msg.GetMessageId()));

  AckMsg ack(ReplicationMessageMetadata(GetNextMessageId()), msg.GetMessageId());
  Send(std::string(zmq_msg.GetRoutingId()), ack, nullptr, zmq_msg.GetSourceCallbackId());
}

void ReplicationManager::NodeConnect(const std::string &node_name, const std::string &hostname, uint16_t port) {
  // Note that creating a Replica will result in a network call.
  UNUSED_ATTRIBUTE auto result = replicas_.try_emplace(node_name, messenger_, node_name, hostname, port);
  NOISEPAGE_ASSERT(result.second, "Failed to connect to a replica?");
}

common::ManagedPointer<messenger::ConnectionId> ReplicationManager::GetNodeConnection(const std::string &replica_name) {
  return replicas_.at(replica_name).GetConnectionId();
}

common::ManagedPointer<PrimaryReplicationManager> ReplicationManager::GetAsPrimary() {
  NOISEPAGE_ASSERT(IsPrimary(), "This should only be called from the primary node!");
  return common::ManagedPointer(this).CastManagedPointerTo<PrimaryReplicationManager>();
}

common::ManagedPointer<ReplicaReplicationManager> ReplicationManager::GetAsReplica() {
  NOISEPAGE_ASSERT(IsReplica(), "This should only be called from a replica node!");
  return common::ManagedPointer(this).CastManagedPointerTo<ReplicaReplicationManager>();
}

void ReplicationManager::BuildReplicationNetwork(const std::string &replication_hosts_path) {
  // The replication.config file is expected to have the following format:
  //   IGNORED LINE (can be used for comments)
  //   REPLICA NAME
  //   REPLICA HOSTNAME
  //   REPLICA PORT
  // Repeated and separated by newlines.
  std::ifstream hosts_file(replication_hosts_path);
  if (!hosts_file.is_open()) {
    throw REPLICATION_EXCEPTION(fmt::format("Unable to open file: {}", replication_hosts_path));
  }
  std::string line;
  std::string replica_name;
  std::string replica_hostname;
  uint16_t replica_port;
  for (int ctr = 0; std::getline(hosts_file, line); ctr = (ctr + 1) % 4) {
    switch (ctr) {
      case 0:
        // Ignored line.
        break;
      case 1:
        replica_name = line;
        break;
      case 2:
        replica_hostname = line;
        break;
      case 3:
        replica_port = std::stoi(line);
        // All information parsed.
        if (identity_ == replica_name) {
          // For our specific identity, check that the port is right.
          NOISEPAGE_ASSERT(replica_port == port_, "Mismatch of identity/port combo in replica config.");
        } else {
          // Connect to the replica.
          NodeConnect(replica_name, replica_hostname, replica_port);
        }
        break;
      default:
        NOISEPAGE_ASSERT(false, "Impossible.");
        break;
    }
  }
  hosts_file.close();
}

void ReplicationManager::Send(const std::string &destination, const BaseReplicationMessage &message,
                              const messenger::CallbackFn &source_callback,
                              messenger::messenger_cb_id_t destination_callback) {
  common::ManagedPointer<messenger::ConnectionId> con_id = GetNodeConnection(destination);

  REPLICATION_LOG_INFO(fmt::format("[SEND] -> {}: {} // PREVIEW {}", destination, message.GetMessageId(),
                                   message.ToJson().dump().substr(0, MESSAGE_PREVIEW_LEN)));

  msg_id_t msg_id = message.GetMessageId();
  pending_msg_mutex_.lock();
  if (pending_msg_.find(msg_id) == pending_msg_.end()) {
    pending_msg_.emplace(msg_id, message);
  }
  pending_msg_.emplace(msg_id, message);
  if (pending_msg_dests_.find(msg_id) == pending_msg_dests_.end()) {
    pending_msg_dests_.emplace(msg_id, std::unordered_set<std::string>{});
  }
  pending_msg_dests_.at(msg_id).emplace(destination);
  pending_msg_mutex_.unlock();

  messenger_->SendMessage(con_id, message.ToJson().dump(), source_callback, destination_callback);
}

void ReplicationManager::Handle(const messenger::ZmqMessage &zmq_msg, const AckMsg &msg) {
  REPLICATION_LOG_TRACE(fmt::format("ACK from {}: {}", zmq_msg.GetRoutingId(), msg.GetMessageAckId()));

  // The callback will have been invoked by the Messenger poll loop already.
  // However, need to clean up the message from the list of pending messages.
  msg_id_t msg_id = msg.GetMessageAckId();
  pending_msg_mutex_.lock();
  std::unordered_set<std::string> &dests = pending_msg_dests_.find(msg_id)->second;
  dests.erase(std::string(zmq_msg.GetRoutingId()));
  if (dests.empty()) {
    pending_msg_dests_.erase(msg_id);
    pending_msg_.erase(msg_id);
  }
  pending_msg_mutex_.unlock();
}

void ReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                   const messenger::ZmqMessage &zmq_msg,
                                   common::ManagedPointer<BaseReplicationMessage> msg) {
  switch (msg->GetMessageType()) {
    case ReplicationMessageType::ACK: {
      Handle(zmq_msg, *msg.CastManagedPointerTo<AckMsg>());
      break;
    }
    default: {
      throw REPLICATION_EXCEPTION(fmt::format("Not sure how to handle in ReplicationManager: {}",
                                              ReplicationMessageTypeToString(msg->GetMessageType())));
    }
  }
}

}  // namespace noisepage::replication
