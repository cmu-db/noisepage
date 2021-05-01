#include "replication/replication_messages.h"

#include "common/json.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::replication {

// All the string keys. Hopefully having them in one place minimizes conflict.

const char *ReplicationMessageMetadata::key_message_id = "message_id";
const char *BaseReplicationMessage::key_message_type = "message_type";
const char *BaseReplicationMessage::key_metadata = "metadata";
const char *NotifyOATMsg::key_batch_id = "oat_batch";
const char *NotifyOATMsg::key_oldest_active_txn = "oat_ts";
const char *RecordsBatchMsg::key_batch_id = "batch_id";
const char *RecordsBatchMsg::key_contents = "contents";
const char *TxnAppliedMsg::key_applied_txn_id = "applied_txn_id";

// MessageWrapper

MessageWrapper::MessageWrapper() : underlying_message_(std::make_unique<MessageFormat>()) {}

MessageWrapper::MessageWrapper(std::string_view str)
    : underlying_message_(std::make_unique<MessageFormat>(common::json::from_msgpack(str))) {}

template <typename T>
void MessageWrapper::Put(const char *key, T value) {
  (*underlying_message_)[key] = value;
}
template void MessageWrapper::Put<std::string>(const char *key, std::string value);
template void MessageWrapper::Put<std::vector<uint8_t>>(const char *key, std::vector<uint8_t> value);
template void MessageWrapper::Put<MessageWrapper>(const char *key, MessageWrapper value);
template void MessageWrapper::Put<record_batch_id_t>(const char *key, record_batch_id_t value);
template void MessageWrapper::Put<msg_id_t>(const char *key, msg_id_t value);
template void MessageWrapper::Put<transaction::timestamp_t>(const char *key, transaction::timestamp_t value);

template <typename T>
T MessageWrapper::Get(const char *key) const {
  return underlying_message_->at(key).get<T>();
}
template std::string MessageWrapper::Get<std::string>(const char *key) const;
template std::vector<uint8_t> MessageWrapper::Get<std::vector<uint8_t>>(const char *key) const;
template MessageWrapper MessageWrapper::Get<MessageWrapper>(const char *key) const;
template record_batch_id_t MessageWrapper::Get<record_batch_id_t>(const char *key) const;
template msg_id_t MessageWrapper::Get<msg_id_t>(const char *key) const;
template transaction::timestamp_t MessageWrapper::Get<transaction::timestamp_t>(const char *key) const;

std::string MessageWrapper::Serialize() const {
  const auto msg_pack = common::json::to_msgpack(*underlying_message_);
  return std::string(reinterpret_cast<const char *>(msg_pack.data()), msg_pack.size());
}

common::json MessageWrapper::ToJson() const { return *underlying_message_; }

void MessageWrapper::FromJson(const common::json &j) { *(this->underlying_message_) = j; }

// ReplicationMessageMetadata

MessageWrapper ReplicationMessageMetadata::ToMessageWrapper() const {
  MessageWrapper message;
  message.Put(key_message_id, msg_id_);
  return message;
}

ReplicationMessageMetadata::ReplicationMessageMetadata(const MessageWrapper &message)
    : msg_id_(message.Get<msg_id_t>(key_message_id)) {}

ReplicationMessageMetadata::ReplicationMessageMetadata(msg_id_t msg_id) : msg_id_(msg_id) {}

// BaseReplicationMessage

MessageWrapper BaseReplicationMessage::ToMessageWrapper() const {
  MessageWrapper message;
  message.Put(key_message_type, ReplicationMessageTypeToString(type_));
  // Nested fields need to be the same type as underlying message format because MessageWrapper isn't automatically
  // serialized
  message.Put(key_metadata, metadata_.ToMessageWrapper());
  return message;
}

std::string BaseReplicationMessage::Serialize() const { return ToMessageWrapper().Serialize(); }

BaseReplicationMessage::BaseReplicationMessage(const MessageWrapper &message)
    : type_(ReplicationMessageTypeFromString(message.Get<std::string>(key_message_type))),
      metadata_(ReplicationMessageMetadata(message.Get<MessageWrapper>(key_metadata))) {}

BaseReplicationMessage::BaseReplicationMessage(ReplicationMessageType type, ReplicationMessageMetadata metadata)
    : type_(type), metadata_(metadata) {}

DEFINE_JSON_BODY_DECLARATIONS(MessageWrapper);

// NotifyOATMsg

MessageWrapper NotifyOATMsg::ToMessageWrapper() const {
  MessageWrapper message = BaseReplicationMessage::ToMessageWrapper();
  message.Put(key_batch_id, batch_id_);
  message.Put(key_oldest_active_txn, oldest_active_txn_);
  return message;
}

NotifyOATMsg::NotifyOATMsg(const MessageWrapper &message)
    : BaseReplicationMessage(message),
      batch_id_(message.Get<record_batch_id_t>(key_batch_id)),
      oldest_active_txn_(message.Get<transaction::timestamp_t>(key_oldest_active_txn)) {}

NotifyOATMsg::NotifyOATMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
                           transaction::timestamp_t oldest_active_txn)
    : BaseReplicationMessage(ReplicationMessageType::NOTIFY_OAT, metadata),
      batch_id_(batch_id),
      oldest_active_txn_(oldest_active_txn) {}

// RecordsBatchMsg

MessageWrapper RecordsBatchMsg::ToMessageWrapper() const {
  MessageWrapper message = BaseReplicationMessage::ToMessageWrapper();
  message.Put(key_batch_id, batch_id_);
  message.Put(key_contents, contents_);
  return message;
}

RecordsBatchMsg::RecordsBatchMsg(const MessageWrapper &message)
    : BaseReplicationMessage(message),
      batch_id_(message.Get<record_batch_id_t>(key_batch_id)),
      contents_(message.Get<std::string>(key_contents)) {}

RecordsBatchMsg::RecordsBatchMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
                                 storage::BufferedLogWriter *buffer)
    : BaseReplicationMessage(ReplicationMessageType::RECORDS_BATCH, metadata),
      batch_id_(batch_id),
      contents_(std::string(buffer->buffer_, buffer->buffer_size_)) {}

// TxnAppliedMsg

MessageWrapper TxnAppliedMsg::ToMessageWrapper() const {
  MessageWrapper message = BaseReplicationMessage::ToMessageWrapper();
  message.Put(key_applied_txn_id, applied_txn_id_);
  return message;
}

TxnAppliedMsg::TxnAppliedMsg(const MessageWrapper &message)
    : BaseReplicationMessage(message), applied_txn_id_(message.Get<transaction::timestamp_t>(key_applied_txn_id)) {}

TxnAppliedMsg::TxnAppliedMsg(ReplicationMessageMetadata metadata, transaction::timestamp_t applied_txn_id)
    : BaseReplicationMessage(ReplicationMessageType::TXN_APPLIED, metadata), applied_txn_id_(applied_txn_id) {}

std::unique_ptr<BaseReplicationMessage> BaseReplicationMessage::ParseFromString(std::string_view str) {
  MessageWrapper message(str);
  // BaseReplicationMessage switches on the message's key_message_type to figure out what type of message to create.
  ReplicationMessageType msg_type = ReplicationMessageTypeFromString(message.Get<std::string>(key_message_type));
  switch (msg_type) {
    // clang-format off
    case ReplicationMessageType::NOTIFY_OAT:          { return std::make_unique<NotifyOATMsg>(message); }
    case ReplicationMessageType::RECORDS_BATCH:       { return std::make_unique<RecordsBatchMsg>(message); }
    case ReplicationMessageType::TXN_APPLIED:         { return std::make_unique<TxnAppliedMsg>(message); }
    case ReplicationMessageType::INVALID:             // Fall-through.
    case ReplicationMessageType::NUM_ENUM_ENTRIES:
      throw REPLICATION_EXCEPTION("Got an INVALID ReplicationMessage?");
      // clang-format on
  }
  throw REPLICATION_EXCEPTION("This case is impossible but GCC can't figure it out.");
}

}  // namespace noisepage::replication
