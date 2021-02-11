#include "replication/replication_manager.h"

#include <chrono>
#include <fstream>
#include <optional>

#include "common/error/exception.h"
#include "common/json.h"
#include "loggers/replication_logger.h"
#include "network/network_io_utils.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace {

bool CompareJsonBuffer(nlohmann::json left, nlohmann::json right) { return left.at("buf_id") > right.at("buf_id"); }

}  // namespace

namespace noisepage::replication {

Replica::Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
                 const std::string &hostname, uint16_t port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, port)),
      connection_(messenger->MakeConnection(replica_info_)),
      last_heartbeat_(0) {}

ReplicationManager::ReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : messenger_(messenger),
      identity_(network_identity),
      port_(port),
      provider_(std::make_unique<storage::ReplicationLogProvider>(std::chrono::seconds(1), false)),
      received_buffer_queue_(CompareJsonBuffer),
      empty_buffer_queue_(empty_buffer_queue) {
  // TODO(WAN): provider_ arguments
  auto listen_destination = messenger::ConnectionDestination::MakeTCP("", "127.0.0.1", port);
  messenger_->ListenForConnection(listen_destination, network_identity,
                                  [this](common::ManagedPointer<messenger::Messenger> messenger,
                                         const messenger::ZmqMessage &msg) { EventLoop(messenger, msg); });
  BuildReplicaList(replication_hosts_path);

  // TODO(WAN): development purposes
  replication_logger->set_level(spdlog::level::trace);
}

ReplicationManager::~ReplicationManager() = default;

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
  if (!replication_enabled_) {
    REPLICATION_LOG_WARN(fmt::format("Skipping send -> {} as replication is disabled."));
    return;
  }

  REPLICATION_LOG_TRACE(fmt::format("Send -> {} (type {} block {}): msg size {}", replica_name,
                                    static_cast<uint8_t>(type), block, msg.size()));
  bool completed = false;
  try {
    messenger_->SendMessage(
        GetReplicaConnection(replica_name), msg,
        [this, block, &completed](common::ManagedPointer<messenger::Messenger> messenger,
                                  const messenger::ZmqMessage &msg) {
          if (block) {
            // If this isn't a blocking send, then completed will fall out of scope.
            completed = true;
            cvar_.notify_all();
          }
        },
        static_cast<uint64_t>(type));

    if (block) {
      std::unique_lock<std::mutex> lock(mutex_);
      // If the caller requested to block until the operation was completed, the thread waits.
      cvar_.wait(lock, [&completed] { return completed; });
      lock.unlock();
    }
  } catch (const MessengerException &e) {
    REPLICATION_LOG_WARN(fmt::format("[FAILED] Send -> {} (type {} block {}): msg size {}", replica_name,
                                     static_cast<uint8_t>(type), block, msg.size()));
  }
}

void ReplicationManager::ReplicateBuffer(storage::BufferedLogWriter *buffer) {
  if (!replication_enabled_) {
    REPLICATION_LOG_WARN(fmt::format("Skipping replicate buffer as replication is disabled."));
    return;
  }

  NOISEPAGE_ASSERT(buffer != nullptr,
                   "Don't try to replicate null buffers. That's pointless."
                   "You might plausibly want to track statistics at some point, but that should not happen here.");
  // TODO(WAN): Is it true that a replica will never want to send its buffers to the primary?
  // TODO(WAN): Cache the identity check into a bool?
  if (identity_ == "primary") {
    common::json j;
    // TODO(WAN): Add a size and checksum to message.
    j["buf_id"] = buffer_id_++;
    j["content"] = nlohmann::json::to_cbor(std::string(buffer->buffer_, buffer->buffer_size_));

    for (const auto &replica : replicas_) {
      ReplicaSend(replica.first, MessageType::REPLICATE_BUFFER, j.dump(), true);
    }
  }

  if (buffer->MarkSerialized()) {
    empty_buffer_queue_->Enqueue(buffer);
  }
}

void ReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                   const messenger::ZmqMessage &msg) {
  switch (static_cast<MessageType>(msg.GetDestinationCallbackId())) {
    case MessageType::ACK: {
      nlohmann::json json = nlohmann::json::parse(msg.GetMessage());
      REPLICATION_LOG_TRACE(fmt::format("ACK: {}", json.dump()));
      auto callback_id = json.at("callback_id").get<uint64_t>();
      auto callback = messenger->GetCallback(callback_id);
      (*callback)(messenger, msg);
      break;
    }
    case MessageType::HEARTBEAT:
      REPLICATION_LOG_TRACE(fmt::format("Heartbeat from: {}", msg.GetRoutingId()));
      break;
    case MessageType::REPLICATE_BUFFER: {
      // Parse the buffer from the received message and add it to the provided replication logs.
      {
        nlohmann::json message = nlohmann::json::parse(msg.GetMessage());
        // TODO(WAN): Sanity-check the received message.

        uint64_t msg_id = message.at("buf_id");
        REPLICATION_LOG_TRACE(fmt::format("ReplicateBuffer from: {} {}", msg.GetRoutingId(), msg_id));

        // Check if the message needs to be buffered.
        if (msg_id > last_buffer_id_ + 1) {
          // The message should be buffered if there are gaps in between the last seen buffer.
          received_buffer_queue_.push(message);
        } else {
          // Otherwise, pull out the log record from the message and hand them to the replication log provider.
          std::string content = nlohmann::json::from_cbor(message["content"].get<std::vector<uint8_t>>());
          provider_->AddBufferFromMessage(content);
          last_buffer_id_ = msg_id;
          // This may unleash the rest of the buffered messages.
          while (!received_buffer_queue_.empty()) {
            message = received_buffer_queue_.top();
            msg_id = message.at("buf_id");
            // Stop once you're missing a buffer.
            if (msg_id > last_buffer_id_ + 1) {
              break;
            }
            // Otherwise, send the top buffer's contents along.
            received_buffer_queue_.pop();
            NOISEPAGE_ASSERT(msg_id == last_buffer_id_ + 1, "Duplicate buffer? Old buffer?");
            content = nlohmann::json::from_cbor(message["content"].get<std::vector<uint8_t>>());
            provider_->AddBufferFromMessage(content);
            last_buffer_id_ = msg_id;
          }
        }
      }
      // TODO(WAN): Is it actually necessary to buffer the message first? A crashed replica is indistinguishable from
      //  a replica that just came up right? If buffering is not necessary, move this up so that replying is faster.
      //  We want to reply as quickly as possible -- the sender may be waiting for our confirmation.
      // Acknowledge receipt of the buffer to the sender.
      // TODO(WAN): Add a size and checksum to message.
      {
        common::json j;
        j["callback_id"] = msg.GetSourceCallbackId();
        ReplicaSend(std::string(msg.GetRoutingId()), MessageType::ACK, j.dump(), false);
      }
      break;
    }
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
      REPLICATION_LOG_TRACE(
          fmt::format("Replica {}: heartbeat initialized at {}.", replica_name, replica.last_heartbeat_));
    }
  }

  REPLICATION_LOG_TRACE(fmt::format("Replica {}: heartbeat start.", replica_name));
  try {
    messenger_->SendMessage(
        GetReplicaConnection(replica_name), "",
        [&replica_name, &replica](common::ManagedPointer<messenger::Messenger> messenger,
                                  const messenger::ZmqMessage &msg) {
          auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
          auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
          replica.last_heartbeat_ = epoch_now_ms.count();
          REPLICATION_LOG_TRACE(fmt::format("Replica {}: last heartbeat {}, heartbeat {} OK.", replica_name,
                                            replica.last_heartbeat_, epoch_now_ms.count()));
        },
        static_cast<uint64_t>(MessageType::HEARTBEAT));
  } catch (const MessengerException &e) {
    REPLICATION_LOG_TRACE(
        fmt::format("Replica {}: last heartbeat {}, heartbeat failed.", replica_name, replica.last_heartbeat_));
    auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
    auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
    if (epoch_now_ms.count() - replica.last_heartbeat_ >= REPLICATION_CARDIAC_ARREST_MS) {
      REPLICATION_LOG_WARN(fmt::format("Replica {}: last heartbeat {}, declared dead {}.", replica_name,
                                       replica.last_heartbeat_, epoch_now_ms.count()));
    }
  }
  REPLICATION_LOG_TRACE(fmt::format("Replica {}: heartbeat end.", replica_name));
}

common::ManagedPointer<messenger::ConnectionId> ReplicationManager::GetReplicaConnection(
    const std::string &replica_name) {
  return replicas_.at(replica_name).GetConnectionId();
}

}  // namespace noisepage::replication