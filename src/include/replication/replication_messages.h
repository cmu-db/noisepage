#pragma once

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/enum_defs.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "messenger/messenger_defs.h"
#include "replication/replication_defs.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
class BufferedLogWriter;
}  // namespace noisepage::storage

namespace noisepage::replication {

#define REPLICATION_MESSAGE_TYPE_ENUM(T)                                                    \
  /** Invalid message type (for uninitialized or invalid state only!). */                   \
  T(ReplicationMessageType, INVALID)                                                        \
  /** Primary notifying the replica of the oldest active txn time. */                       \
  T(ReplicationMessageType, NOTIFY_OAT)                                                     \
  /** Primary sending the replica a batch of log records.*/                                 \
  T(ReplicationMessageType, RECORDS_BATCH)                                                  \
  /** Replica notifying the primary that the replica has applied a specific transaction. */ \
  T(ReplicationMessageType, TXN_APPLIED)

/** The type of message that is being sent. */
ENUM_DEFINE(ReplicationMessageType, uint8_t, REPLICATION_MESSAGE_TYPE_ENUM);
#undef REPLICATION_MESSAGE_TYPE_ENUM

/** Abstraction over the underlying format used to send replication messages over the network */
class MessageWrapper {
 public:
  /** The underlying format of messages used in replication */
  using MessageFormat = common::json;

  /** Default constructor */
  MessageWrapper();

  /** Constructor which parses a string */
  explicit MessageWrapper(std::string_view str);

  /**
   * Adds a value to the message with a specific key
   *
   * @tparam T type of value to add
   * @param key key of value in message
   * @param value value to add to message
   */
  template <typename T>
  void Put(const char *key, T value);

  /**
   * Get a value from the message with specific key
   *
   * @tparam T type of value to get
   * @param key key of value
   * @return value from message with specified key
   */
  template <typename T>
  T Get(const char *key) const;

  /**
   * Serialize the message
   *
   * @return serialized version of message
   */
  std::string Serialize() const;

  /**
   * Converts MessageWrapper to JSON
   * @return JSON version of MessageWrapper
   */
  common::json ToJson() const;

  /**
   * Converts JSON to MessageWrapper
   * @param j JSON to convert
   */
  void FromJson(const common::json &j);

 private:
  std::unique_ptr<MessageFormat> underlying_message_;
};

DEFINE_JSON_HEADER_DECLARATIONS(MessageWrapper);

/** ReplicationMessageMetadata contains all of the metadata that every type of BaseReplicationMessage should contain. */
class ReplicationMessageMetadata {
 public:
  /** Constructor (to send). */
  explicit ReplicationMessageMetadata(msg_id_t msg_id);
  /** Constructor (to receive). */
  explicit ReplicationMessageMetadata(const MessageWrapper &message);

  /** @return     The MessageWrapper form of this metadata. */
  MessageWrapper ToMessageWrapper() const;

  /** @return     The ID of the message. */
  msg_id_t GetMessageId() const { return msg_id_; }

 private:
  static const char *key_message_id;  ///< JSON key for the message ID.
  msg_id_t msg_id_;                   ///< The ID of this message.
};

/** Base class for all replicated messages. */
class BaseReplicationMessage {
 public:
  /** Destructor. */
  virtual ~BaseReplicationMessage() = default;

  /** @return     The type of replication message that this is. */
  virtual ReplicationMessageType GetMessageType() const { return type_; }

  /** @return     Serialized form of this message. */
  std::string Serialize() const;

  /** @return     The parsed replication message. */
  static std::unique_ptr<BaseReplicationMessage> ParseFromString(std::string_view str);

  /** @return     The metadata for this message. */
  const ReplicationMessageMetadata &GetMetadata() const { return metadata_; }

  /** @return     The message ID from the metadata. Convenience function. */
  msg_id_t GetMessageId() const { return GetMetadata().GetMessageId(); }

 protected:
  /** Constructor (to send). */
  explicit BaseReplicationMessage(ReplicationMessageType type, ReplicationMessageMetadata metadata);
  /** Constructor (to receive). */
  explicit BaseReplicationMessage(const MessageWrapper &message);
  /** Converts message into MessageWrapper form */
  virtual MessageWrapper ToMessageWrapper() const;

 private:
  static const char *key_message_type;  ///< JSON key for the message type.
  static const char *key_metadata;      ///< JSON key for the message metadata.

  ReplicationMessageType type_;          ///< The type of this message.
  ReplicationMessageMetadata metadata_;  ///< The metadata for this message.
};

/**
 * NotifyOATMsg is sent from primary -> replicas.
 * This is used to notify the replicas that the oldest active transaction on the primary has been updated.
 */
class NotifyOATMsg : public BaseReplicationMessage {
 public:
  /**
   * Constructor (to send).
   *
   * @param metadata            The metadata of the message.
   * @param batch_id            The ID for the last sent batch of log records.
   * @param oldest_active_txn   The oldest active txn time.
   */
  NotifyOATMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id,
               transaction::timestamp_t oldest_active_txn);
  /** Constructor (to receive). */
  explicit NotifyOATMsg(const MessageWrapper &message);
  /** Destructor. */
  ~NotifyOATMsg() override = default;

  ReplicationMessageType GetMessageType() const override { return ReplicationMessageType::NOTIFY_OAT; }

  /** @return The ID of the last batch of log records that was sent. */
  record_batch_id_t GetBatchId() const { return batch_id_; }

  /** @return The oldest active transaction of the OAT. */
  transaction::timestamp_t GetOldestActiveTxn() const { return oldest_active_txn_; }

 protected:
  MessageWrapper ToMessageWrapper() const override;

 private:
  static const char *key_batch_id;           ///< JSON key for the batch ID.
  static const char *key_oldest_active_txn;  ///< JSON key for the oldest active transaction.

  record_batch_id_t batch_id_;  ///< The batch ID identifies the batch that must be received before applying this OAT.
  transaction::timestamp_t oldest_active_txn_;  ///< Oldest active transaction.
};

/**
 * RecordsBatchMsg is sent from primary -> replica, containing a batch of log records to be applied.
 * Note that the log records in the same batch are not necessarily from the same transaction.
 */
class RecordsBatchMsg : public BaseReplicationMessage {
 public:
  /**
   * Constructor (to send).
   *
   * @param metadata            The metadata of the message.
   * @param batch_id            The ID for this batch of log records.
   * @param buffer              The contents of this batch of log records.
   */
  RecordsBatchMsg(ReplicationMessageMetadata metadata, record_batch_id_t batch_id, storage::BufferedLogWriter *buffer);
  /** Constructor (to receive). */
  explicit RecordsBatchMsg(const MessageWrapper &message);
  /** Destructor. */
  ~RecordsBatchMsg() override = default;

  ReplicationMessageType GetMessageType() const override { return ReplicationMessageType::RECORDS_BATCH; }

  /** @return The ID of this batch of log records. */
  record_batch_id_t GetBatchId() const { return batch_id_; }

  /** @return The contents of this batch of log records. */
  std::string GetContents() const { return contents_; }

  /** @return The batch ID that should appear after the given batch ID. */
  static record_batch_id_t NextBatchId(record_batch_id_t batch_id) {
    if (batch_id.UnderlyingValue() == std::numeric_limits<uint64_t>::max()) {
      return record_batch_id_t{NULL_ID + 1};
    }
    return record_batch_id_t{batch_id.UnderlyingValue() + 1};
  }

 protected:
  MessageWrapper ToMessageWrapper() const override;

 private:
  static const char *key_batch_id;  ///< JSON key for the batch ID.
  static const char *key_contents;  ///< JSON key for the contents.

  record_batch_id_t batch_id_;  ///< The batch ID identifies the order of records sent by the remote origin.
  std::string contents_;        ///< The actual contents of the buffer.
};

/** TxnAppliedMsg is sent from replica -> primary, indicating that a given transaction has been successfully applied. */
class TxnAppliedMsg : public BaseReplicationMessage {
 public:
  /** Constructor (to send). */
  explicit TxnAppliedMsg(ReplicationMessageMetadata metadata, transaction::timestamp_t applied_txn_id);
  /** Constructor (to receive). */
  explicit TxnAppliedMsg(const MessageWrapper &message);
  /** Destructor. */
  ~TxnAppliedMsg() override = default;

  ReplicationMessageType GetMessageType() const override { return ReplicationMessageType::TXN_APPLIED; }

  /** @return The ID of the transaction that was applied on the replica. */
  transaction::timestamp_t GetAppliedTxnId() const { return applied_txn_id_; }

 protected:
  MessageWrapper ToMessageWrapper() const override;

 private:
  static const char *key_applied_txn_id;     ///< JSON key for the applied transaction ID.
  transaction::timestamp_t applied_txn_id_;  ///< The ID of the transaction that was applied on the replica.
};

}  // namespace noisepage::replication
