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

// MessageFacade

DEFINE_JSON_BODY_DECLARATIONS(MessageFacade);

// ReplicationMessageMetadata

MessageFacade ReplicationMessageMetadata::ToMessageFacade() const {
  MessageFacade message;
  message.Put(key_message_id, msg_id_);
  return message;
}

ReplicationMessageMetadata::ReplicationMessageMetadata(const MessageFacade &message)
    : msg_id_(message.Get<msg_id_t>(key_message_id)) {}

ReplicationMessageMetadata::ReplicationMessageMetadata(msg_id_t msg_id) : msg_id_(msg_id) {}

// BaseReplicationMessage

MessageFacade BaseReplicationMessage::ToMessageFacade() const {
  MessageFacade message;
  message.Put(key_message_type, ReplicationMessageTypeToString(type_));
  // Nested fields need to be the same type as underlying message format because MessageFacade isn't automatically
  // serialized
  message.Put(key_metadata, metadata_.ToMessageFacade());
  return message;
}

std::string BaseReplicationMessage::Serialize() const { return ToMessageFacade().Serialize(); }

BaseReplicationMessage::BaseReplicationMessage(const MessageFacade &message)
    : type_(ReplicationMessageTypeFromString(message.Get<const std::string>(key_message_type))),
      metadata_(ReplicationMessageMetadata(message.Get<MessageFacade>(key_metadata))) {}

BaseReplicationMessage::BaseReplicationMessage(ReplicationMessageType type, ReplicationMessageMetadata metadata)
    : type_(type), metadata_(metadata) {}

// NotifyOATMsg

MessageFacade NotifyOATMsg::ToMessageFacade() const {
  MessageFacade message = BaseReplicationMessage::ToMessageFacade();
  message.Put(key_batch_id, batch_id_);
  message.Put(key_oldest_active_txn, oldest_active_txn_);
  return message;
}

NotifyOATMsg::NotifyOATMsg(const MessageFacade &message)
    : BaseReplicationMessage(message),
      batch_id_(message.Get<record_batch_id_t>(key_batch_id)),
      oldest_active_txn_(message.Get<transaction::timestamp_t>(key_oldest_active_txn)) {}

NotifyOATMsg::NotifyOATMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
                           transaction::timestamp_t oldest_active_txn)
    : BaseReplicationMessage(ReplicationMessageType::NOTIFY_OAT, metadata),
      batch_id_(batch_id),
      oldest_active_txn_(oldest_active_txn) {}

// RecordsBatchMsg

MessageFacade RecordsBatchMsg::ToMessageFacade() const {
  MessageFacade message = BaseReplicationMessage::ToMessageFacade();
  message.Put(key_batch_id, batch_id_);
  message.Put(key_contents, MessageFacade::ToCbor(contents_));
  return message;
}

RecordsBatchMsg::RecordsBatchMsg(const MessageFacade &message)
    : BaseReplicationMessage(message),
      batch_id_(message.Get<record_batch_id_t>(key_batch_id)),
      contents_(MessageFacade::FromCbor(message.Get<std::vector<uint8_t>>(key_contents))) {}

RecordsBatchMsg::RecordsBatchMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
                                 storage::BufferedLogWriter *buffer)
    : BaseReplicationMessage(ReplicationMessageType::RECORDS_BATCH, metadata),
      batch_id_(batch_id),
      contents_(std::string(buffer->buffer_, buffer->buffer_size_)) {}

// TxnAppliedMsg

MessageFacade TxnAppliedMsg::ToMessageFacade() const {
  MessageFacade message = BaseReplicationMessage::ToMessageFacade();
  message.Put(key_applied_txn_id, applied_txn_id_);
  return message;
}

TxnAppliedMsg::TxnAppliedMsg(const MessageFacade &message)
    : BaseReplicationMessage(message), applied_txn_id_(message.Get<transaction::timestamp_t>(key_applied_txn_id)) {}

TxnAppliedMsg::TxnAppliedMsg(ReplicationMessageMetadata metadata, transaction::timestamp_t applied_txn_id)
    : BaseReplicationMessage(ReplicationMessageType::TXN_APPLIED, metadata), applied_txn_id_(applied_txn_id) {}

std::unique_ptr<BaseReplicationMessage> BaseReplicationMessage::ParseFromString(std::string_view str) {
  MessageFacade message(str);
  // BaseReplicationMessage switches on the messages's key_message_type to figure out what type of message to create.
  ReplicationMessageType msg_type = ReplicationMessageTypeFromString(message.Get<const std::string>(key_message_type));
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
