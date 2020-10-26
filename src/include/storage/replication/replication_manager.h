#pragma once

#include <fstream>
#include "common/container/concurrent_queue.h"
#include "storage/recovery/replication_log_provider.h"
#include "network/network_io_utils.h"
#include "messenger/messenger.h"
#include "messenger/connection_destination.h"
#include "common/json.h"
#include "loggers/storage_logger.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace terrier::storage {

class ReplicationManager {
public:
  // Each line in the config file should be formatted as ip:port.
  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger) : messenger_(messenger) {
    // Read from config file.
    /*std::ifstream replica_config(config_path);
    if (replica_config.is_open()) {
      std::getline(replica_config, master_address_);
      std::getline(replica_config, replica_address_);
    } else {
      STORAGE_LOG_ERROR("Replica config file is missing.");
    }*/
  }

  void SetReplicationLogProvider(common::ManagedPointer<storage::ReplicationLogProvider> provider) {
    provider_ = provider;
  }

  messenger::ConnectionId Connect(const std::string& destination_ip, int destination_port) {
    destination_ip_ = destination_ip;
    if (destination_port == 9022) {
      destination_port_ = 9023;
    } else {
      destination_port_ = 9022;
    }
    auto destination = messenger::ConnectionDestination::MakeTCP("replica", destination_ip_, destination_port_);
    return messenger_->MakeConnection(destination);
  }

  void AddRecordBuffer(BufferedLogWriter *network_buffer) {
    replication_consumer_queue_.Enqueue(std::make_pair(network_buffer, std::vector<CommitCallback>()));
  }

  // Serialize log record buffer to json and send the message across the network.
  bool SendMessage(common::ManagedPointer<messenger::Messenger> messenger, messenger::ConnectionId &target) {
    // Grab buffers in queue.
    std::deque<BufferedLogWriter*> temp_buffer_queue;
    uint64_t data_size = 0;
    SerializedLogs logs;
    while (!replication_consumer_queue_.Empty()) {
      replication_consumer_queue_.Dequeue(&logs);
      data_size += logs.first->GetBufferSize();
      temp_buffer_queue.push_back(logs.first);
    }
    //TERRIER_ASSERT(data_size > 0, "Amount of data to send must be greater than 0");

    // Build JSON object.
    nlohmann::json j;
    j["type"] = "itp";
\
    std::string content;
    uint64_t size = 0;
    for (auto *buffer : temp_buffer_queue) {
      size += buffer->GetBufferSize();
      content += buffer->GetBuffer();
    }

    j["size"] = size;
    j["content"] = nlohmann::json::to_cbor(content);
    STORAGE_LOG_ERROR("ok1" + std::to_string(size));

    // Send the message.
    bool message_sent = false;
    messenger->SendMessage(common::ManagedPointer(&target), j.dump(),
        [&message_sent](std::string_view sender_id, std::string_view message) {
            STORAGE_LOG_ERROR("invoked");
            message_sent = true;
          },
          static_cast<uint8_t>(messenger::Messenger::BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1);
    return message_sent;
  }

  // Fills content of message into buffer.
  void RecvMessage(const nlohmann::json& message, std::unique_ptr<network::ReadBuffer>& buffer) {
    size_t size = message["size"];
    std::vector<uint8_t> content_raw = message["content"];
    std::string content = nlohmann::json::from_cbor(content_raw);
    STORAGE_LOG_ERROR("ok2" + std::to_string(size));
    std::vector<unsigned char> content_buffer(content.begin(), content.end());
    network::ReadBufferView view(size, content_buffer.begin());
    buffer->FillBufferFrom(view, size);
  }

  void Recover(const std::string& string_view) {
    nlohmann::json message = nlohmann::json::parse(string_view);
    size_t size = message["size"];
    std::unique_ptr<network::ReadBuffer> buffer(new network::ReadBuffer(size));
    RecvMessage(message, buffer);
    provider_->HandBufferToReplication(std::move(buffer));
  }

private:
  //std::string master_address_;
  //std::string replica_address_;
  std::string destination_ip_;
  int destination_port_;
  common::ConcurrentQueue<SerializedLogs> replication_consumer_queue_;
  common::ManagedPointer<messenger::Messenger> messenger_;
  common::ManagedPointer<storage::ReplicationLogProvider> provider_;
};

}  // namespace terrier::storage;