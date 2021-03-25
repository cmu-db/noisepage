#include "replication/replication_messages.h"

#include "common/json.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::replication {

// All the string keys. Hopefully having them in one place minimizes conflict.

const char *ReplicationMessageMetadata::key_message_id = "message_id";
const char *BaseReplicationMessage::key_message_type = "message_type";
const char *BaseReplicationMessage::key_metadata = "metadata";
const char *AckMsg::key_message_ack_id = "message_ack_id";
const char *RecordsBatchMsg::key_batch_id = "batch_id";
const char *RecordsBatchMsg::key_contents = "contents";
const char *TxnAppliedMsg::key_applied_txn_id = "applied_txn_id";

// ReplicationMessageMetadata

common::json ReplicationMessageMetadata::ToJson() const {
  common::json json;
  json[key_message_id] = msg_id_;
  return json;
}

ReplicationMessageMetadata::ReplicationMessageMetadata(const common::json &json) : msg_id_(json[key_message_id]) {}

ReplicationMessageMetadata::ReplicationMessageMetadata(msg_id_t msg_id) : msg_id_(msg_id) {}

// BaseReplicationMessage

common::json BaseReplicationMessage::ToJson() const {
  common::json json;
  json[key_message_type] = ReplicationMessageTypeToString(type_);
  json[key_metadata] = metadata_.ToJson();
  return json;
}

BaseReplicationMessage::BaseReplicationMessage(const common::json &json)
    : type_(ReplicationMessageTypeFromString(json[key_message_type].get<std::string>())),
      metadata_(ReplicationMessageMetadata(json[key_metadata].get<common::json>())) {}

BaseReplicationMessage::BaseReplicationMessage(ReplicationMessageType type, ReplicationMessageMetadata metadata)
    : type_(type), metadata_(metadata) {}

// AckMsg

common::json AckMsg::ToJson() const {
  common::json json = BaseReplicationMessage::ToJson();
  json[key_message_ack_id] = message_ack_id_;
  return json;
}

AckMsg::AckMsg(const common::json &json)
    : BaseReplicationMessage(json), message_ack_id_(json[key_message_ack_id].get<msg_id_t>()) {}

AckMsg::AckMsg(ReplicationMessageMetadata metadata, msg_id_t message_ack_id)
    : BaseReplicationMessage(ReplicationMessageType::ACK, metadata), message_ack_id_(message_ack_id) {}

// RecordsBatchMsg

common::json RecordsBatchMsg::ToJson() const {
  common::json json = BaseReplicationMessage::ToJson();
  json[key_batch_id] = batch_id_;
  json[key_contents] = nlohmann::json::to_cbor(contents_);
  return json;
}

RecordsBatchMsg::RecordsBatchMsg(const common::json &json)
    : BaseReplicationMessage(json),
      batch_id_(json[key_batch_id].get<record_batch_id_t>()),
      contents_(nlohmann::json::from_cbor(json[key_contents].get<std::vector<uint8_t>>())) {}

RecordsBatchMsg::RecordsBatchMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
                                 storage::BufferedLogWriter *buffer)
    : BaseReplicationMessage(ReplicationMessageType::RECORDS_BATCH, metadata),
      batch_id_(batch_id),
      contents_(std::string(buffer->buffer_, buffer->buffer_size_)) {}

// TxnAppliedMsg

common::json TxnAppliedMsg::ToJson() const {
  common::json json = BaseReplicationMessage::ToJson();
  json[key_applied_txn_id] = applied_txn_id_;
  return json;
}

TxnAppliedMsg::TxnAppliedMsg(const common::json &json)
    : BaseReplicationMessage(json), applied_txn_id_(json[key_applied_txn_id].get<transaction::timestamp_t>()) {}

TxnAppliedMsg::TxnAppliedMsg(ReplicationMessageMetadata metadata, transaction::timestamp_t applied_txn_id)
    : BaseReplicationMessage(ReplicationMessageType::TXN_APPLIED, metadata), applied_txn_id_(applied_txn_id) {}

std::unique_ptr<BaseReplicationMessage> BaseReplicationMessage::ParseFromJson(const common::json &json) {
  // BaseReplicationMessage switches on the json's key_message_type to figure out what type of message to create.
  ReplicationMessageType msg_type = ReplicationMessageTypeFromString(json[key_message_type]);
  switch (msg_type) {
      // clang-format off
    case ReplicationMessageType::ACK:                 { return std::make_unique<AckMsg>(json); }
    case ReplicationMessageType::RECORDS_BATCH:       { return std::make_unique<RecordsBatchMsg>(json); }
    case ReplicationMessageType::TXN_APPLIED:         { return std::make_unique<TxnAppliedMsg>(json); }
    case ReplicationMessageType::INVALID:             // fall-through
    case ReplicationMessageType::NUM_ENUM_ENTRIES:
      throw REPLICATION_EXCEPTION("Got an INVALID ReplicationMessage?");
      // clang-format on
  }
  throw REPLICATION_EXCEPTION("This case is impossible but GCC can't figure it out.");
}

}  // namespace noisepage::replication
