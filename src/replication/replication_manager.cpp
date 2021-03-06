#include "replication/replication_manager.h"

#include <chrono>  // NOLINT
#include <fstream>
#include <optional>

#include "common/error/exception.h"
#include "common/json.h"
#include "loggers/replication_logger.h"
#include "network/network_io_utils.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace {

/** @return True if left > right. False otherwise. */
bool CompareMessages(const noisepage::replication::ReplicateBufferMessage &left,
                     const noisepage::replication::ReplicateBufferMessage &right) {
  return left.GetMessageId() > right.GetMessageId();
}

}  // namespace

namespace noisepage::replication {

Replica::Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
                 const std::string &hostname, uint16_t port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, port)),
      connection_(messenger->MakeConnection(replica_info_)),
      last_heartbeat_(0) {}

const char *ReplicateBufferMessage::key_buf_id = "buf_id";
const char *ReplicateBufferMessage::key_content = "content";

ReplicateBufferMessage ReplicateBufferMessage::FromMessage(const messenger::ZmqMessage &msg) {
  // TODO(WAN): Sanity-check the received message.
  nlohmann::json message = nlohmann::json::parse(msg.GetMessage());
  uint64_t source_callback_id = msg.GetSourceCallbackId();
  uint64_t buffer_id = message.at(key_buf_id);
  std::string contents = nlohmann::json::from_cbor(message[key_content].get<std::vector<uint8_t>>());
  return ReplicateBufferMessage(buffer_id, std::move(contents), source_callback_id);
}

std::string ReplicateBufferMessage::ToString() {
  common::json json;
  json[key_buf_id] = buffer_id_;
  json[key_content] = nlohmann::json::to_cbor(contents_);
  // TODO(WAN): Add a size and checksum to message.
  return json.dump();
}

ReplicationManager::ReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : messenger_(messenger), identity_(network_identity), port_(port), empty_buffer_queue_(empty_buffer_queue) {
  auto listen_destination = messenger::ConnectionDestination::MakeTCP("", "127.0.0.1", port);
  messenger_->ListenForConnection(listen_destination, network_identity,
                                  [this](common::ManagedPointer<messenger::Messenger> messenger,
                                         const messenger::ZmqMessage &msg) { EventLoop(messenger, msg); });
  BuildReplicaList(replication_hosts_path);
}

ReplicationManager::~ReplicationManager() = default;

void ReplicationManager::EnableReplication() {
  REPLICATION_LOG_TRACE(fmt::format("[PID={}] Replication enabled.", ::getpid()));
  replication_enabled_ = true;
}

void ReplicationManager::DisableReplication() {
  REPLICATION_LOG_TRACE(fmt::format("[PID={}] Replication disabled.", ::getpid()));
  replication_enabled_ = false;
}

void ReplicationManager::BuildReplicaList(const std::string &replication_hosts_path) {
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
            blocking_send_cvar_.notify_all();
          }
        },
        static_cast<uint64_t>(type));

    if (block) {
      std::unique_lock<std::mutex> lock(blocking_send_mutex_);
      // If the caller requested to block until the operation was completed, the thread waits.
      if (!blocking_send_cvar_.wait_for(lock, REPLICATION_MAX_BLOCKING_WAIT_TIME, [&completed] { return completed; })) {
        // TODO(WAN): Additionally, this is hackily a messenger exception so that it gets handled by the catch.
        throw MESSENGER_EXCEPTION("TODO(WAN): Handle a replica dying in synchronous replication.");
      }
    }
  } catch (const MessengerException &e) {
    // TODO(WAN): This assumes that the replica has died. If the replica has in fact not died, and somehow the message
    //  just crapped out and failed to send, this will hang the replica since the replica expects messages to be
    //  received in order and we don't try to resend the message.
    REPLICATION_LOG_WARN(fmt::format("[FAILED] Send -> {} (type {} block {}): msg size {}", replica_name,
                                     static_cast<uint8_t>(type), block, msg.size()));
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
    case MessageType::HEARTBEAT: {
      REPLICATION_LOG_TRACE(fmt::format("Heartbeat from: {}", msg.GetRoutingId()));
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
  }
  auto epoch_now = std::chrono::system_clock::now().time_since_epoch();
  auto epoch_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(epoch_now);
  if (epoch_now_ms.count() - replica.last_heartbeat_ >= REPLICATION_CARDIAC_ARREST_MS) {
    REPLICATION_LOG_WARN(fmt::format("Replica {}: last heartbeat {}, declared dead {}.", replica_name,
                                     replica.last_heartbeat_, epoch_now_ms.count()));
  }
  REPLICATION_LOG_TRACE(fmt::format("Replica {}: heartbeat end.", replica_name));
}

common::ManagedPointer<messenger::ConnectionId> ReplicationManager::GetReplicaConnection(
    const std::string &replica_name) {
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

const char *PrimaryReplicationManager::primary_identity = "primary";

PrimaryReplicationManager::PrimaryReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue) {}

PrimaryReplicationManager::~PrimaryReplicationManager() = default;

void PrimaryReplicationManager::ReplicateBuffer(storage::BufferedLogWriter *buffer) {
  if (!replication_enabled_) {
    REPLICATION_LOG_WARN(fmt::format("Skipping replicate buffer as replication is disabled."));
    return;
  }

  NOISEPAGE_ASSERT(buffer != nullptr,
                   "Don't try to replicate null buffers. That's pointless."
                   "You might plausibly want to track statistics at some point, but that should not happen here.");
  ReplicateBufferMessage msg{next_buffer_sent_id_++, std::string(buffer->buffer_, buffer->buffer_size_)};
  std::string msg_as_string = msg.ToString();

  for (const auto &replica : replicas_) {
    // TODO(WAN): many things break when block is flipped from true to false.
    ReplicaSend(replica.first, MessageType::REPLICATE_BUFFER, msg_as_string, true);
  }

  if (buffer->MarkSerialized()) {
    empty_buffer_queue_->Enqueue(buffer);
  }
}

ReplicaReplicationManager::ReplicaReplicationManager(
    common::ManagedPointer<messenger::Messenger> messenger, const std::string &network_identity, uint16_t port,
    const std::string &replication_hosts_path,
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
    : ReplicationManager(messenger, network_identity, port, replication_hosts_path, empty_buffer_queue),
      provider_(std::make_unique<storage::ReplicationLogProvider>(std::chrono::seconds(1))),
      received_message_queue_(CompareMessages) {}

ReplicaReplicationManager::~ReplicaReplicationManager() = default;

void ReplicaReplicationManager::HandleReplicatedBuffer(const messenger::ZmqMessage &msg) {
  auto rb_msg = ReplicateBufferMessage::FromMessage(msg);
  REPLICATION_LOG_TRACE(fmt::format("ReplicateBuffer from: {} {}", msg.GetRoutingId(), rb_msg.GetMessageId()));

  // Check if the message needs to be buffered.
  if (rb_msg.GetMessageId() > last_record_received_id_ + 1) {
    // The message should be buffered if there are gaps in between the last seen buffer.
    received_message_queue_.push(rb_msg);
  } else {
    // Otherwise, pull out the log record from the message and hand the record to the replication log provider.
    provider_->AddBufferFromMessage(rb_msg.GetSourceCallbackId(), rb_msg.GetContents());
    last_record_received_id_ = rb_msg.GetMessageId();
    // This may unleash the rest of the buffered messages.
    while (!received_message_queue_.empty()) {
      auto &top = received_message_queue_.top();
      // Stop once you're missing a buffer.
      if (top.GetMessageId() > last_record_received_id_ + 1) {
        break;
      }
      // Otherwise, send the top buffer's contents along.
      received_message_queue_.pop();
      NOISEPAGE_ASSERT(top.GetMessageId() == last_record_received_id_ + 1, "Duplicate buffer? Old buffer?");
      provider_->AddBufferFromMessage(top.GetSourceCallbackId(), top.GetContents());
      last_record_received_id_ = top.GetMessageId();
    }
  }
}

void ReplicaReplicationManager::EventLoop(common::ManagedPointer<messenger::Messenger> messenger,
                                          const messenger::ZmqMessage &msg) {
  switch (static_cast<MessageType>(msg.GetDestinationCallbackId())) {
    case MessageType::REPLICATE_BUFFER: {
      HandleReplicatedBuffer(msg);
      break;
    }
    default: {
      // Delegate to the common ReplicationManager event loop.
      ReplicationManager::EventLoop(messenger, msg);
      break;
    }
  }
}

}  // namespace noisepage::replication