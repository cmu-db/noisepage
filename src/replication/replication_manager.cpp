#include "replication/replication_manager.h"

#include <chrono>
#include <fstream>
#include <optional>

#include "common/error/exception.h"
#include "loggers/replication_logger.h"

namespace noisepage::replication {

Replica::Replica(common::ManagedPointer<noisepage::messenger::Messenger> messenger, const std::string &replica_name,
                 const std::string &hostname, uint16_t port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, port)),
      connection_(messenger->MakeConnection(replica_info_)),
      last_heartbeat_(0) {}

ReplicationManager::ReplicationManager(common::ManagedPointer<noisepage::messenger::Messenger> messenger,
                                       const std::string &network_identity, uint16_t port,
                                       const std::string &replication_hosts_path,
                                       common::ManagedPointer<storage::ReplicationLogProvider> provider)
    : messenger_(messenger), identity_(network_identity), port_(port), provider_(provider) {
  auto listen_destination = messenger::ConnectionDestination::MakeTCP("", "127.0.0.1", port);
  messenger_->ListenForConnection(listen_destination, network_identity,
                                  [this](common::ManagedPointer<messenger::Messenger> messenger,
                                         const messenger::ZmqMessage &msg) { EventLoop(messenger, msg); });
  BuildReplicaList(replication_hosts_path);
  for (const auto &replica : replicas_) {
    ReplicaHeartbeat(replica.first);
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));
  for (const auto &replica : replicas_) {
    ReplicaHeartbeat(replica.first);
  }
}

void ReplicationManager::BuildReplicaList(const std::string &replication_hosts_path) {
  // The replication_hosts.conf file is expected to have the following format:
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
          ReplicaConnect(replica_name, replica_hostname, replica_port);
        }
        break;
      default:
        NOISEPAGE_ASSERT(false, "Impossible.");
        break;
    }
  }
  hosts_file.close();
}

void ReplicationManager::ReplicaConnect(const std::string &replica_name, const std::string &hostname, uint16_t port) {
  replicas_.try_emplace(replica_name, messenger_, replica_name, hostname, port);
}

void ReplicationManager::ReplicaSend(const std::string &replica_name, const ReplicationManager::MessageType type,
                                     const std::string &msg, bool block) {
  std::unique_lock<std::mutex> lock(mutex_);
  bool completed = false;
  messenger_->SendMessage(
      GetReplicaConnection(replica_name), msg,
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

void ReplicationManager::EventLoop(common::ManagedPointer<noisepage::messenger::Messenger> messenger,
                                   const noisepage::messenger::ZmqMessage &msg) {
  switch (static_cast<MessageType>(msg.GetDestinationCallbackId())) {
    case MessageType::HEARTBEAT:
      REPLICATION_LOG_INFO(fmt::format("Heartbeat from: {}", msg.GetRoutingId()));
      break;
    default:
      break;
  }
}

void ReplicationManager::ReplicaHeartbeat(const std::string &replica_name) {
  Replica &replica = replicas_.at(replica_name);

  // If the replica's heartbeat time has not been initialized yet, set the heartbeat time to the current time.
  {
    auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
    auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
    if (0 == replica.last_heartbeat_) {
      replica.last_heartbeat_ = epoch_now_ms.count();
    }
  }

  REPLICATION_LOG_INFO(fmt::format("Replica {}: heartbeat start.", replica_name));
  try {
    messenger_->SendMessage(
        GetReplicaConnection(replica_name), "",
        [&replica_name, &replica](common::ManagedPointer<messenger::Messenger> messenger,
                                  const messenger::ZmqMessage &msg) {
          auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
          auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
          replica.last_heartbeat_ = epoch_now_ms.count();
          REPLICATION_LOG_INFO(fmt::format("Replica {}: last heartbeat {}, heartbeat {} OK.", replica_name,
                                           replica.last_heartbeat_, epoch_now_ms.count()));
        },
        static_cast<uint64_t>(MessageType::HEARTBEAT));
  } catch (const MessengerException &e) {
    REPLICATION_LOG_INFO(
        fmt::format("Replica {}: last heartbeat {}, heartbeat failed.", replica_name, replica.last_heartbeat_));
    auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
    auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
    if (epoch_now_ms.count() - replica.last_heartbeat_ >= REPLICATION_CARDIAC_ARREST_MS) {
      REPLICATION_LOG_INFO(fmt::format("Replica {}: last heartbeat {}, declared dead {}.", replica_name,
                                       replica.last_heartbeat_, epoch_now_ms.count()));
    }
  }
  REPLICATION_LOG_INFO(fmt::format("Replica {}: heartbeat end.", replica_name));
}

void ReplicationManager::AddLogRecordBuffer(storage::BufferedLogWriter *network_buffer) {
  replication_consumer_queue_.Enqueue(std::make_pair(network_buffer, std::vector<storage::CommitCallback>()));
}

common::ManagedPointer<noisepage::messenger::ConnectionId> ReplicationManager::GetReplicaConnection(
    const std::string &replica_name) {
  return replicas_.at(replica_name).GetConnectionId();
}

nlohmann::json ReplicationManager::SerializeLogRecords() {
  // Grab buffers in queue.
  std::deque<storage::BufferedLogWriter *> temp_buffer_queue;
  uint64_t data_size = 0;
  storage::SerializedLogs logs;
  while (!replication_consumer_queue_.Empty()) {
    replication_consumer_queue_.Dequeue(&logs);
    data_size += logs.first->GetBufferSize();
    temp_buffer_queue.push_back(logs.first);
  }

  // Build JSON object.
  nlohmann::json j;
  j[replication_message_identifier_] = true;

  std::string message_content;
  uint64_t message_size = 0;
  for (auto *buffer : temp_buffer_queue) {
    message_size += buffer->GetBufferSize();
    message_content += buffer->GetBuffer();
  }

  j["size"] = message_size;
  j["content"] = nlohmann::json::to_cbor(message_content);
  STORAGE_LOG_INFO("Sending replication message of size ", message_size);

  return j;
}

void ReplicationManager::RecoverFromSerializedLogRecords(const std::string &string_view) {
  // Parse the message.
  nlohmann::json message = nlohmann::json::parse(string_view);

  // Get the original replication info.
  size_t message_size = message["size"];

  std::unique_ptr<network::ReadBuffer> buffer(new network::ReadBuffer(message_size));
  std::vector<uint8_t> message_content_raw = message["content"];
  std::string message_content = nlohmann::json::from_cbor(message_content_raw);

  // Fill in a ReadBuffer for converting to log record.
  std::vector<unsigned char> message_content_buffer(message_content.begin(), message_content.end());
  network::ReadBufferView view(message_size, message_content_buffer.begin());
  buffer->FillBufferFrom(view, message_size);

  // Pass to log provider for recovery.
  provider_->HandBufferToReplication(std::move(buffer));
}

}  // namespace noisepage::replication