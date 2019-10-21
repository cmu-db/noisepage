#pragma once

#include <memory>
#include <string>
#include <vector>

#include "network/packet_writer.h"

namespace terrier::network {
/**
 * Wrapper around an I/O layer WriteQueue to provide Postgres-specific
 * helper methods.
 */
class PostgresPacketWriter : public PacketWriter {
 public:
  /**
   * Instantiates a new PostgresPacketWriter backed by the given WriteQueue
   */
  explicit PostgresPacketWriter(const std::shared_ptr<WriteQueue> &write_queue) : PacketWriter(write_queue) {}

  /**
   * Write out a packet with a single type that is associated with SSL.
   * (PG_SSL_YES, PG_SSL_NO)
   * @param type Type of message to write out
   */
  void WriteSSLPacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    TERRIER_ASSERT(IsPacketEmpty(), "packet length is null");
    switch (type) {
      case NetworkMessageType::PG_SSL_YES:
      case NetworkMessageType::PG_SSL_NO:
        WriteType(type);
        break;
      default:
        BeginPacket(type).EndPacket();
    }
  }

  /**
   * Writes a simple query
   * @param query string to execute
   */
  void WriteSimpleQuery(const std::string &query) {
    BeginPacket(NetworkMessageType::PG_SIMPLE_QUERY_COMMAND).AppendString(query).EndPacket();
  }

  /**
   * Writes an empty query response
   */
  void WriteEmptyQueryResponse() { BeginPacket(NetworkMessageType::PG_EMPTY_QUERY_RESPONSE).EndPacket(); }

  /**
   * Writes a no-data response
   */
  void WriteNoData() { BeginPacket(NetworkMessageType::PG_NO_DATA_RESPONSE).EndPacket(); }

  /**
   * Writes parameter description (used in Describe command)
   * @param param_types The types of the parameters in the statement
   */
  void WriteParameterDescription(const std::vector<PostgresValueType> &param_types) {
    BeginPacket(NetworkMessageType::PG_PARAMETER_DESCRIPTION);
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
    BeginPacket(NetworkMessageType::PG_ROW_DESCRIPTION).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
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

    BeginPacket(NetworkMessageType::PG_DATA_ROW).AppendValue<int16_t>(static_cast<int16_t>(values.size()));
    for (auto &value : values) {
      // use text to represent values for now
      std::string ret;
      if (value.Type() == TypeId::INTEGER)
        ret = std::to_string(TransientValuePeeker::PeekInteger(value));
      else if (value.Type() == TypeId::DECIMAL)
        ret = std::to_string(TransientValuePeeker::PeekDecimal(value));
      else if (value.Type() == TypeId::VARCHAR)
        ret = TransientValuePeeker::PeekVarChar(value);
      if (ret == "NULL") {
        AppendValue<int32_t>(static_cast<int32_t>(-1));
      } else {
        AppendValue<int32_t>(static_cast<int32_t>(ret.length())).AppendString(ret, false);
      }
    }
    EndPacket();
  }

  /**
   * Tells the client that the query command is complete.
   * @param tag records the which kind of query it is. (INSERT? DELETE? SELECT?) and the number of rows.
   */
  void WriteCommandComplete(const std::string &tag) {
    BeginPacket(NetworkMessageType::PG_COMMAND_COMPLETE).AppendString(tag).EndPacket();
  }

  /**
   * Writes a parse message packet
   * @param destinationStmt The name of the destination statement to parse
   * @param query The query string to be parsed
   * @param params Supplied parameter object types in the query
   */
  void WriteParseCommand(const std::string &destinationStmt, const std::string &query,
                         const std::vector<int32_t> &params) {
    PacketWriter &writer = BeginPacket(NetworkMessageType::PG_PARSE_COMMAND)
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
    PacketWriter &writer = BeginPacket(NetworkMessageType::PG_BIND_COMMAND)
                               .AppendString(destinationPortal)
                               .AppendString(sourcePreparedStmt);
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
    BeginPacket(NetworkMessageType::PG_EXECUTE_COMMAND).AppendString(portal).AppendValue(rowLimit).EndPacket();
  }

  /**
   * Writes a Sync message packet
   */
  void WriteSyncCommand() { BeginPacket(NetworkMessageType::PG_SYNC_COMMAND).EndPacket(); }

  /**
   * Writes a Describe message packet
   * @param type The type of object to describe
   * @param objectName The name of the object to describe8
   */
  void WriteDescribeCommand(DescribeCommandObjectType type, const std::string &objectName) {
    BeginPacket(NetworkMessageType::PG_DESCRIBE_COMMAND).AppendRawValue(type).AppendString(objectName).EndPacket();
  }

  /**
   * Writes a Close command on an object
   * @param type The type of object to close
   * @param objectName The name of the object to close
   */
  void WriteCloseCommand(DescribeCommandObjectType type, const std::string &objectName) {
    BeginPacket(NetworkMessageType::PG_CLOSE_COMMAND).AppendRawValue(type).AppendString(objectName).EndPacket();
  }

  /**
   * Tells the client that the parse command is complete.
   */
  void WriteParseComplete() { BeginPacket(NetworkMessageType::PG_PARSE_COMPLETE).EndPacket(); }

  /**
   * Tells the client that the bind command is complete.
   */
  void WriteBindComplete() { BeginPacket(NetworkMessageType::PG_BIND_COMPLETE).EndPacket(); }
};

}  // namespace terrier::network
