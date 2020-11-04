#pragma once

#include "storage/replication/replication_manager.h"

namespace noisepage::storage {

  void ReplicationManager::AddRecordBuffer(BufferedLogWriter *network_buffer) {
    replication_consumer_queue_.Enqueue(std::make_pair(network_buffer, std::vector<CommitCallback>()));
  }

  bool ReplicationManager::SendMessage(messenger::ConnectionId &target) {
    // Grab buffers in queue.
    std::deque<BufferedLogWriter*> temp_buffer_queue;
    uint64_t data_size = 0;
    SerializedLogs logs;
    while (!replication_consumer_queue_.Empty()) {
      replication_consumer_queue_.Dequeue(&logs);
      data_size += logs.first->GetBufferSize();
      temp_buffer_queue.push_back(logs.first);
    }

    // Build JSON object.
    nlohmann::json j;
    j["type"] = "itp";

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
    messenger_->SendMessage(common::ManagedPointer(&target), j.dump(),
                           [&message_sent](std::string_view sender_id, std::string_view message) {
                             STORAGE_LOG_ERROR("invoked");
                             message_sent = true;
                           },
                           static_cast<uint8_t>(messenger::Messenger::BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1);
    return message_sent;
  }

  void ReplicationManager::Recover(const std::string& string_view) {
    // Parse the message.
    nlohmann::json message = nlohmann::json::parse(string_view);
    size_t size = message["size"];
    std::unique_ptr<network::ReadBuffer> buffer(new network::ReadBuffer(size));
    std::vector<uint8_t> content_raw = message["content"];
    std::string content = nlohmann::json::from_cbor(content_raw);
    std::vector<unsigned char> content_buffer(content.begin(), content.end());
    network::ReadBufferView view(size, content_buffer.begin());
    buffer->FillBufferFrom(view, size);

    // Pass to log provider.
    provider_->HandBufferToReplication(std::move(buffer));
  }

}  // namespace noisepage::storage;