#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "network/network_defs.h"
#include "network/network_io_utils.h"
#include "type/transient_value_peeker.h"

namespace terrier::network {

// TODO(Tianyu): It looks very broken that this never changes.
// clang-format off

 /**
  * Hardcoded server parameter values to send to the client
  */
  const std::unordered_map<std::string, std::string>
    PARAMETER_STATUS_MAP = {
      {"application_name", "psql"},
      {"client_encoding", "UTF8"},
      {"DateStyle", "ISO, MDY"},
      {"integer_datetimes", "on"},
      {"IntervalStyle", "postgres"},
      {"is_superuser", "on"},
      {"server_encoding", "UTF8"},
      {"server_version", "9.5devel"},
      {"session_authorization", "postgres"},
      {"standard_conforming_strings", "on"},
      {"TimeZone", "US/Eastern"}
  };
// clang-format on

/**
 * Encapsulates an input packet
 */
struct PostgresInputPacket {
  /**
   * Type of message this packet encodes
   */
  NetworkMessageType msg_type_ = NetworkMessageType::NULL_COMMAND;

  /**
   * Length of this packet's contents
   */
  size_t len_ = 0;

  /**
   * ReadBuffer containing this packet's contents
   */
  std::shared_ptr<ReadBuffer> buf_;

  /**
   * Whether or not this packet's header has been parsed yet
   */
  bool header_parsed_ = false;

  /**
   * Whether or not this packet's buffer was extended
   */
  bool extended_ = false;

  /**
   * Constructs an empty PostgresInputPacket
   */
  PostgresInputPacket() = default;

  /**
   * Constructs an empty PostgresInputPacket
   */
  PostgresInputPacket(const PostgresInputPacket &) = default;

  /**
   * Constructs an empty PostgresInputPacket
   */
  PostgresInputPacket(PostgresInputPacket &&) = default;

  /**
   * Clears the packet's contents
   */
  void Clear() {
    msg_type_ = NetworkMessageType::NULL_COMMAND;
    len_ = 0;
    buf_ = nullptr;
    header_parsed_ = false;
  }
};

/**
 * Postgres Value Types
 * This defines all the types that we will support
 * We do not allow for user-defined types, nor do we try to do anything dynamic.
 * For more information, see 'pg_type.h' in Postgres
 * https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h#L273
 */

enum class PostgresValueType {
  INVALID = INVALID_TYPE_ID,
  BOOLEAN = 16,
  TINYINT = 16,  // BOOLEAN is an alias for TINYINT
  SMALLINT = 21,
  INTEGER = 23,
  VARBINARY = 17,
  BIGINT = 20,
  REAL = 700,
  DOUBLE = 701,
  TEXT = 25,
  BPCHAR = 1042,
  BPCHAR2 = 1014,
  VARCHAR = 1015,
  VARCHAR2 = 1043,
  DATE = 1082,
  TIMESTAMPS = 1114,
  TIMESTAMPS2 = 1184,
  TEXT_ARRAY = 1009,     // TEXTARRAYOID in postgres code
  INT2_ARRAY = 1005,     // INT2ARRAYOID in postgres code
  INT4_ARRAY = 1007,     // INT4ARRAYOID in postgres code
  OID_ARRAY = 1028,      // OIDARRAYOID in postgres code
  FLOADT4_ARRAY = 1021,  // FLOADT4ARRAYOID in postgres code
  DECIMAL = 1700
};

type::TypeId PostgresValueTypeToInternalValueType(PostgresValueType type);

/**
 * Wrapper around an I/O layer WriteQueue to provide Postgres-specific
 * helper methods.
 */
class PostgresPacketWriter {
 public:
  /**
   * Instantiates a new PostgresPacketWriter backed by the given WriteQueue
   */
  explicit PostgresPacketWriter(const std::shared_ptr<WriteQueue> &write_queue) : queue_(*write_queue) {}

  ~PostgresPacketWriter() {
    // Make sure no packet is being written on destruction, otherwise we are
    // malformed write buffer
    TERRIER_ASSERT(curr_packet_len_ == nullptr, "packet length is not null");
  }

  /**
   * Write out a packet with a single type. Some messages will be
   * special cases since no size field is provided. (SSL_YES, SSL_NO)
   * @param type Type of message to write out
   */
  void WriteSingleTypePacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    TERRIER_ASSERT(curr_packet_len_ == nullptr, "packet length is null");
    switch (type) {
      case NetworkMessageType::SSL_YES:
      case NetworkMessageType::SSL_NO:
        queue_.BufferWriteRawValue(type);
        break;
      default:
        BeginPacket(type).EndPacket();
    }
  }

  /**
   * Begin writing a new packet. Caller can use other append methods to write
   * contents to the packet. An explicit call to end packet must be made to
   * make these writes valid.
   * @param type
   * @return self-reference for chaining
   */
  PostgresPacketWriter &BeginPacket(NetworkMessageType type) {
    // No active packet being constructed
    TERRIER_ASSERT(curr_packet_len_ == nullptr, "packet length is null");
    if (type != NetworkMessageType::NO_HEADER) queue_.BufferWriteRawValue(type);
    // Remember the size field since we will need to modify it as we go along.
    // It is important that our size field is contiguous and not broken between
    // two buffers.
    queue_.BufferWriteRawValue<int32_t>(0, false);
    WriteBuffer &tail = *(queue_.buffers_[queue_.buffers_.size() - 1]);
    curr_packet_len_ = reinterpret_cast<uint32_t *>(&tail.buf_[tail.size_ - sizeof(int32_t)]);
    return *this;
  }

  /**
   * Append raw bytes from specified memory location into the write queue.
   * There must be a packet active in the writer.
   * @param src memory location to write from
   * @param len number of bytes to write
   * @return self-reference for chaining
   */
  PostgresPacketWriter &AppendRaw(const void *src, size_t len) {
    TERRIER_ASSERT(curr_packet_len_ != nullptr, "packet length is null");
    queue_.BufferWriteRaw(src, len);
    // Add the size field to the len of the packet. Be mindful of byte
    // ordering. We switch to network ordering only when the packet is finished
    *curr_packet_len_ += static_cast<uint32_t>(len);
    return *this;
  }

  /**
   * Append a value onto the write queue. There must be a packet active in the
   * writer. No byte order conversion is performed. It is up to the caller to
   * do so if needed.
   * @tparam T type of value to write
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PostgresPacketWriter &AppendRawValue(T val) {
    return AppendRaw(&val, sizeof(T));
  }

  /**
   * Append a value of specified length onto the write queue. (1, 2, 4, or 8
   * bytes). It is assumed that these bytes need to be converted to network
   * byte ordering.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PostgresPacketWriter &AppendValue(T val) {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "Invalid size for integer");

    switch (sizeof(T)) {
      case 1:
        return AppendRawValue(val);
      case 2:
        return AppendRawValue(_CAST(T, htobe16(_CAST(uint16_t, val))));
      case 4:
        return AppendRawValue(_CAST(T, htobe32(_CAST(uint32_t, val))));
      case 8:
        return AppendRawValue(_CAST(T, htobe64(_CAST(uint64_t, val))));
        // Will never be here due to compiler optimization
      default:
        throw NETWORK_PROCESS_EXCEPTION("");
    }
  }

  /**
   * Append a string onto the write queue.
   * @param str the string to append
   * @param nul_terminate whether the nul terminaor should be written as well
   * @return self-reference for chaining
   */
  PostgresPacketWriter &AppendString(const std::string &str, bool nul_terminate = true) {
    return AppendRaw(str.data(), nul_terminate ? str.size() + 1 : str.size());
  }

  /**
   * Writes error responses to the client
   * @param error_status The error messages to send
   */
  void WriteErrorResponse(const std::vector<std::pair<NetworkMessageType, std::string>> &error_status) {
    BeginPacket(NetworkMessageType::ERROR_RESPONSE);

    for (const auto &entry : error_status) AppendRawValue(entry.first).AppendString(entry.second);

    // Nul-terminate packet
    AppendRawValue<uchar>(0).EndPacket();
  }

  /**
   * A helper function to write a single error message without having to make a vector every time.
   * @param type
   * @param status
   */
  void WriteSingleErrorResponse(NetworkMessageType type, const std::string &status) {
    std::vector<std::pair<NetworkMessageType, std::string>> buf;
    buf.emplace_back(type, status);
    WriteErrorResponse(buf);
  }

  /**
   * Notify the client a readiness to receive a query
   * @param txn_status
   */
  void WriteReadyForQuery(NetworkTransactionStateType txn_status) {
    BeginPacket(NetworkMessageType::READY_FOR_QUERY).AppendRawValue(txn_status).EndPacket();
  }

  /**
   * Writes response to startup message
   */
  void WriteStartupResponse() {
    BeginPacket(NetworkMessageType::AUTHENTICATION_REQUEST).AppendValue<int32_t>(0).EndPacket();

    for (auto &entry : PARAMETER_STATUS_MAP)
      BeginPacket(NetworkMessageType::PARAMETER_STATUS)
          .AppendString(entry.first)
          .AppendString(entry.second)
          .EndPacket();
    WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  }

  /**
   * Writes the startup message, used by clients
   */
  void WriteStartupRequest(const std::unordered_map<std::string, std::string> &config, int16_t major_version = 3) {
    // Build header, assume minor version is always 0
    BeginPacket(NetworkMessageType::NO_HEADER).AppendValue<int16_t>(major_version).AppendValue<int16_t>(0);
    for (const auto &pair : config) AppendString(pair.first).AppendString(pair.second);
    AppendRawValue<uchar>(0);  // Startup message should have (byte+1) length
    EndPacket();
  }

  /**
   * Writes a simple query
   * @param query string to execute
   */
  void WriteSimpleQuery(const std::string &query) {
    BeginPacket(NetworkMessageType::SIMPLE_QUERY_COMMAND).AppendString(query).EndPacket();
  }

  /**
   * Writes an empty query response
   */
  void WriteEmptyQueryResponse() { BeginPacket(NetworkMessageType::EMPTY_QUERY_RESPONSE).EndPacket(); }

  /**
   * Writes a no-data response
   */
  void WriteNoData() { BeginPacket(NetworkMessageType::NO_DATA_RESPONSE).EndPacket(); }

  /**
   * Writes parameter description (used in Describe command)
   * @param param_types The types of the parameters in the statement
   */
  void WriteParameterDescription(const std::vector<PostgresValueType> &param_types) {
    BeginPacket(NetworkMessageType::PARAMETER_DESCRIPTION);
    AppendValue<int16_t>(static_cast<int16_t>(param_types.size()));

    for (auto &type : param_types) AppendValue<int32_t>(static_cast<int32_t>(type));

    EndPacket();
  }

  /**
   * Writes row description, as the first packet of sending query results
   * @param columns the column names
   */
  void WriteRowDescription(const std::vector<std::string> &columns) {
    // TODO(Weichen): fill correct OIDs here. This depends on the catalog.
    BeginPacket(NetworkMessageType::ROW_DESCRIPTION).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
    for (auto &col_name : columns) {
      AppendString(col_name)
          .AppendValue<int32_t>(0)                                     // table oid, 0 for now
          .AppendValue<int16_t>(0)                                     // column oid, 0 for now
          .AppendValue(static_cast<int32_t>(PostgresValueType::TEXT))  // type oid
          .AppendValue<int16_t>(-1)                                    // Variable Length
          .AppendValue<int32_t>(-1)                                    // pg_attribute.attrmod, generally -1
          .AppendValue<int16_t>(0);                                    // text=0
    }

    EndPacket();
  }

  /**
   * Writes a data row.
   * @param values a row's values.
   */
  void WriteDataRow(const trafficcop::Row &values) {
    using type::TransientValuePeeker;
    using type::TypeId;

    BeginPacket(NetworkMessageType::DATA_ROW).AppendValue<int16_t>(static_cast<int16_t>(values.size()));
    for (auto &value : values) {
      // use text to represent values for now
      std::string ret;
      if (value.Type() == TypeId::INTEGER)
        ret = std::to_string(TransientValuePeeker::PeekInteger(value));
      else if (value.Type() == TypeId::DECIMAL)
        ret = std::to_string(TransientValuePeeker::PeekDecimal(value));
      else if (value.Type() == TypeId::VARCHAR)
        ret = TransientValuePeeker::PeekVarChar(value);

      AppendValue<int32_t>(static_cast<int32_t>(ret.length())).AppendString(ret, false);
    }
    EndPacket();
  }

  /**
   * Tells the client that the query command is complete.
   * @param tag records the which kind of query it is. (INSERT? DELETE? SELECT?) and the number of rows.
   */
  void WriteCommandComplete(const std::string &tag) {
    BeginPacket(NetworkMessageType::COMMAND_COMPLETE).AppendString(tag).EndPacket();
  }

  /**
   * Writes a parse message packet
   * @param destinationStmt The name of the destination statement to parse
   * @param query The query string to be parsed
   * @param params Supplied parameter object types in the query
   */
  void WriteParseCommand(const std::string &destinationStmt, const std::string &query,
                         const std::vector<int32_t> &params) {
    PostgresPacketWriter &writer = BeginPacket(NetworkMessageType::PARSE_COMMAND)
                                       .AppendString(destinationStmt)
                                       .AppendString(query)
                                       .AppendValue(static_cast<int16_t>(params.size()));
    for (auto param : params) {
      writer.AppendValue(param);
    }
    writer.EndPacket();
  }

  /**
   * Writes a Bind message packet
   * @param destinationPortal The portal to bind to
   * @param sourcePreparedStmt The name of the source prepared statement
   * @param paramFormatCodes Binary values format codes describing whether or not the parameters ins paramVals are in
   * text or binary form
   * @param paramVals The parameter values
   * @param resultFormatCodes The format codes to request the results to be formatted to. Same conventions as in
   * paramFormatCodes
   */
  void WriteBindCommand(const std::string &destinationPortal, const std::string &sourcePreparedStmt,
                        std::initializer_list<int16_t> paramFormatCodes,
                        std::initializer_list<std::vector<char> *> paramVals,
                        std::initializer_list<int16_t> resultFormatCodes) {
    PostgresPacketWriter &writer =
        BeginPacket(NetworkMessageType::BIND_COMMAND).AppendString(destinationPortal).AppendString(sourcePreparedStmt);
    writer.AppendValue(static_cast<int16_t>(paramFormatCodes.size()));

    for (auto code : paramFormatCodes) {
      writer.AppendValue(code);
    }
    writer.AppendValue(static_cast<int16_t>(paramVals.size()));

    for (auto param_val : paramVals) {
      if (param_val == nullptr) {
        // NULL value
        writer.AppendValue(static_cast<int32_t>(-1));
        continue;
      }

      auto size = static_cast<int32_t>(param_val->size());
      writer.AppendValue(size);
      writer.AppendRaw(param_val->data(), size);
    }

    writer.AppendValue(static_cast<int16_t>(resultFormatCodes.size()));
    for (auto code : resultFormatCodes) {
      writer.AppendValue(code);
    }
    writer.EndPacket();
  }

  /**
   * Writes an Execute message packet
   * @param portal The name of the portal to execute
   * @param rowLimit Maximum number of rows to return to the client
   */
  void WriteExecuteCommand(const std::string &portal, int32_t rowLimit) {
    BeginPacket(NetworkMessageType::EXECUTE_COMMAND).AppendString(portal).AppendValue(rowLimit).EndPacket();
  }

  /**
   * Writes a Sync message packet
   */
  void WriteSyncCommand() { BeginPacket(NetworkMessageType::SYNC_COMMAND).EndPacket(); }

  /**
   * Writes a Describe message packet
   * @param type The type of object to describe
   * @param objectName The name of the object to describe8
   */
  void WriteDescribeCommand(DescribeCommandObjectType type, const std::string &objectName) {
    BeginPacket(NetworkMessageType::DESCRIBE_COMMAND).AppendRawValue(type).AppendString(objectName).EndPacket();
  }

  /**
   * Writes a Close command on an object
   * @param type The type of object to close
   * @param objectName The name of the object to close
   */
  void WriteCloseCommand(DescribeCommandObjectType type, const std::string &objectName) {
    BeginPacket(NetworkMessageType::CLOSE_COMMAND).AppendRawValue(type).AppendString(objectName).EndPacket();
  }

  /**
   * Tells the client that the parse command is complete.
   */
  void WriteParseComplete() { BeginPacket(NetworkMessageType::PARSE_COMPLETE).EndPacket(); }

  /**
   * Tells the client that the bind command is complete.
   */
  void WriteBindComplete() { BeginPacket(NetworkMessageType::BIND_COMPLETE).EndPacket(); }

  /**
   * End the packet. A packet write must be in progress and said write is not
   * well-formed until this method is called.
   */
  void EndPacket() {
    TERRIER_ASSERT(curr_packet_len_ != nullptr, "packet length is null");
    // Switch to network byte ordering, add the 4 bytes of size field
    *curr_packet_len_ = htonl(*curr_packet_len_ + static_cast<uint32_t>(sizeof(uint32_t)));
    curr_packet_len_ = nullptr;
  }

 private:
  // We need to keep track of the size field of the current packet,
  // so we can update it as more bytes are written into this packet.
  uint32_t *curr_packet_len_ = nullptr;
  // Underlying WriteQueue backing this writer
  WriteQueue &queue_;
};

}  // namespace terrier::network
