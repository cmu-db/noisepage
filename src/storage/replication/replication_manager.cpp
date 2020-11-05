#include "storage/replication/replication_manager.h"

namespace noisepage::storage {

void ReplicationManager::AddLogRecordBuffer(BufferedLogWriter *network_buffer) {
  replication_consumer_queue_.Enqueue(std::make_pair(network_buffer, std::vector<CommitCallback>()));
}

bool ReplicationManager::SendSerializedLogRecords(messenger::ConnectionId &target) {
  // Grab buffers in queue.
  std::deque<BufferedLogWriter *> temp_buffer_queue;
  uint64_t data_size = 0;
  SerializedLogs logs;
  while (!replication_consumer_queue_.Empty()) {
    replication_consumer_queue_.Dequeue(&logs);
    data_size += logs.first->GetBufferSize();
    temp_buffer_queue.push_back(logs.first);
  }

  // Build JSON object.
  nlohmann::json j;
  j[replication_message_identifier] = true;

  std::string message_content;
  uint64_t message_size = 0;
  for (auto *buffer : temp_buffer_queue) {
    message_size += buffer->GetBufferSize();
    message_content += buffer->GetBuffer();
  }

  j["size"] = message_size;
  j["content"] = nlohmann::json::to_cbor(message_content);
  STORAGE_LOG_INFO("Sending replication message of size ", message_size);

  // Send the message.
  bool message_sent = false;
  messenger_->SendMessage(
      common::ManagedPointer(&target), j.dump(),
      [&message_sent](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                      std::string_view message, uint64_t recv_cb_id) {
        STORAGE_LOG_INFO("Receiving replication message from ", sender_id);
        message_sent = true;
      },
      static_cast<uint8_t>(messenger::Messenger::BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1);
  return message_sent;
}

void ReplicationManager::RecoverFromSerliazedLogsRecords(const std::string &string_view) {
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

}  // namespace noisepage::storage