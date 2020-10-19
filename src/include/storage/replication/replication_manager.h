#pragma once

#include <fstream>
#include "common/container/concurrent_queue.h"
#include "storage/recovery/replication_log_provider.h"
#include "network/network_io_utils.h"
#include "messenger/connection_destination.h"
#include "common/json.h"
#include "loggers/storage_logger.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "messenger/messenger.h"

namespace terrier::storage {

class ReplicationManager {
public:
  // Each line in the config file should be formatted as ip:port.
  ReplicationManager(common::ManagedPointer<messenger::Messenger> messenger, const std::string& config_path, const std::string& destination_ip, int destination_port) : messenger_(messenger), destination_id_(Connect(destination_ip, destination_port)) {
    // Read from config file.
    /*std::ifstream replica_config(config_path);
    if (replica_config.is_open()) {
      std::getline(replica_config, master_address_);
      std::getline(replica_config, replica_address_);
    } else {
      STORAGE_LOG_ERROR("Replica config file is missing.");
    }*/
  }

  messenger::ConnectionId Connect(const std::string& destination_ip, int destination_port) {
    destination_ip_ = destination_ip;
    if (destination_port == 9022) {
      destination_port_ = 9023;
    } else {
      destination_port_ = 9022;
    }
    auto destination = messenger::ConnectionDestination::MakeTCP(destination_ip_, destination_port_);
    messenger_->ListenForConnection(destination);
    return messenger_->MakeConnection(destination, destination_ip_ + std::to_string(destination_port_));
  }

  void AddRecordBuffer(BufferedLogWriter *network_buffer) {
    replication_consumer_queue_.Enqueue(std::make_pair(network_buffer, std::vector<CommitCallback>()));
  }

  // Serialize log record buffer to json and send the message across the network.
  void SendMessage() {
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
    STORAGE_LOG_ERROR("ok" + std::to_string(size));

    // Send the message.
    messenger_->SendMessage(common::ManagedPointer<messenger::ConnectionId>(&destination_id_), j.dump());
  }

  // Fills content of message into buffer.
  void RecvMessage(nlohmann::json message, std::unique_ptr<network::ReadBuffer>& buffer) {
    //size_t size = message["size"];
    /*std::string content = nlohmann::json::from_cbor(message["content"]);
    std::vector<unsigned char> content_buffer(content.begin(), content.end());
    network::ReadBufferView view(size, content_buffer.begin());
    buffer->FillBufferFrom(view, size);*/
  }

  void Recover(nlohmann::json message) {
    size_t size = message["size"];
    std::unique_ptr<network::ReadBuffer> buffer(new network::ReadBuffer(size));
    RecvMessage(message, buffer);
    //provider_->HandBufferToReplication(buffer);
  }

private:
  //std::string master_address_;
  //std::string replica_address_;
  std::string destination_ip_;
  int destination_port_;
  common::ConcurrentQueue<SerializedLogs> replication_consumer_queue_;
  common::ManagedPointer<messenger::Messenger> messenger_;
  messenger::ConnectionId destination_id_;
  //common::ManagedPointer<storage::ReplicationLogProvider> provider_;
};

}  // namespace terrier::storage;