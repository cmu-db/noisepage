#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/sql/value.h"
#include "network/packet_writer.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::network {

/**
 * The string value to use for 'true' boolean values
 */
constexpr char POSTGRES_BOOLEAN_STR_TRUE[] = "t";

/**
 * The string value to use for 'false' boolean values
 */
constexpr char POSTGRES_BOOLEAN_STR_FALSE[] = "f";

/**
 * Wrapper around an I/O layer WriteQueue to provide Postgres-specific
 * helper methods.
 */
class PostgresPacketWriter : public PacketWriter {
 public:
  /**
   * Normal constructor for PostgresPacketWriter
   * @param write_queue backing data structure for this packet writer
   */
  explicit PostgresPacketWriter(const common::ManagedPointer<WriteQueue> write_queue) : PacketWriter(write_queue) {}

  /**
   * Constructor if you want to override the result wire format
   * @param write_queue backing data structure for this packet writer
   * @param format text or binary format
   */
  PostgresPacketWriter(const common::ManagedPointer<WriteQueue> write_queue, const FieldFormat format)
      : PacketWriter(write_queue), format_(format) {}

  /**
   * Writes error responses to the client
   * @param error_status The error messages to send
   */
  void WriteErrorResponse(const std::vector<std::pair<NetworkMessageType, std::string>> &error_status) {
    BeginPacket(NetworkMessageType::PG_ERROR_RESPONSE);

    for (const auto &entry : error_status) AppendRawValue(entry.first).AppendString(entry.second);

    // Nul-terminate packet
    AppendRawValue<uchar>(0).EndPacket();
  }

  /**
   * Notify the client a readiness to receive a query
   * @param txn_status
   */
  void WriteReadyForQuery(NetworkTransactionStateType txn_status) {
    BeginPacket(NetworkMessageType::PG_READY_FOR_QUERY).AppendRawValue(txn_status).EndPacket();
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
   * Writes response to startup message
   */
  void WriteStartupResponse() {
    BeginPacket(NetworkMessageType::PG_AUTHENTICATION_REQUEST).AppendValue<int32_t>(0).EndPacket();

    for (auto &entry : PG_PARAMETER_STATUS_MAP)
      BeginPacket(NetworkMessageType::PG_PARAMETER_STATUS)
          .AppendString(entry.first)
          .AppendString(entry.second)
          .EndPacket();
    WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  }

  /**
   * Writes a simple query
   * @param query string to execute
   */
  void WriteSimpleQuery(const std::string &query) {
    BeginPacket(NetworkMessageType::PG_SIMPLE_QUERY_COMMAND).AppendString(query).EndPacket();
  }

  /**
   * Writes a Postgres notice response
   * @param message human readable message
   */
  void WriteNoticeResponse(const std::string &message) {
    BeginPacket(NetworkMessageType::PG_NOTICE_RESPONSE)
        .AppendRawValue(NetworkMessageType::PG_HUMAN_READABLE_ERROR)
        .AppendString(message)
        .AppendRawValue<uchar>(0)
        .EndPacket();  // Nul-terminate packet
  }

  /**
   * Writes a Postgres error response
   * @param message human readable message
   */
  void WriteErrorResponse(const std::string &message) {
    BeginPacket(NetworkMessageType::PG_ERROR_RESPONSE)
        .AppendRawValue(NetworkMessageType::PG_HUMAN_READABLE_ERROR)
        .AppendString(message)
        .AppendRawValue<uchar>(0)
        .EndPacket();  // Nul-terminate packet
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
   * @param columns the column information from the OutputSchema
   */
  void WriteRowDescription(const std::vector<planner::OutputSchema::Column> &columns) {
    BeginPacket(NetworkMessageType::PG_ROW_DESCRIPTION).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
    for (const auto &col : columns) {
      const auto col_type = col.GetType();
      // TODO(Matt): Figure out how to get table oid and column oids in the OutputSchema (Optimizer's job?)
      AppendString(col.GetName())
          .AppendValue<int32_t>(0)  // table oid (if it's a column from a table), 0 otherwise
          .AppendValue<int16_t>(0)  // column oid (if it's a column from a table), 0 otherwise
          .AppendValue(static_cast<int32_t>(InternalValueTypeToPostgresValueType(col_type)));  // type oid
      if (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY) {
        AppendValue<int16_t>(-1);  // variable length
      } else {
        AppendValue<int16_t>(type::TypeUtil::GetTypeSize(col_type));  // data type size
      }
      AppendValue<int32_t>(-1)  // type modifier, generally -1 (see pg_attribute.atttypmod)
          .AppendValue<int16_t>(static_cast<int16_t>(format_));  // format code for the field, 0 for text, 1 for binary
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
   * Writes Postgres command complete
   * @param query_type what type of query this was
   * @param num_rows number of rows for the queries that need it in their output
   */
  void WriteCommandComplete(const QueryType query_type, const uint32_t num_rows) {
    switch (query_type) {
      case QueryType::QUERY_BEGIN:
        WriteCommandComplete("BEGIN");
        break;
      case QueryType::QUERY_COMMIT:
        WriteCommandComplete("COMMIT");
        break;
      case QueryType::QUERY_ROLLBACK:
        WriteCommandComplete("ROLLBACK");
        break;
      case QueryType::QUERY_INSERT:
        WriteCommandComplete("INSERT 0 " + std::to_string(num_rows));
        break;
      case QueryType::QUERY_DELETE:
        WriteCommandComplete("DELETE " + std::to_string(num_rows));
        break;
      case QueryType::QUERY_UPDATE:
        WriteCommandComplete("UPDATE " + std::to_string(num_rows));
        break;
      case QueryType::QUERY_SELECT:
        WriteCommandComplete("SELECT " + std::to_string(num_rows));
        break;
      case QueryType::QUERY_CREATE_DB:
        WriteCommandComplete("CREATE DATABASE");
        break;
      case QueryType::QUERY_CREATE_TABLE:
        WriteCommandComplete("CREATE TABLE");
        break;
      case QueryType::QUERY_CREATE_INDEX:
        WriteCommandComplete("CREATE INDEX");
        break;
      case QueryType::QUERY_CREATE_SCHEMA:
        WriteCommandComplete("CREATE SCHEMA");
        break;
      case QueryType::QUERY_DROP_DB:
        WriteCommandComplete("DROP DATABASE");
        break;
      case QueryType::QUERY_DROP_TABLE:
        WriteCommandComplete("DROP TABLE");
        break;
      case QueryType::QUERY_DROP_INDEX:
        WriteCommandComplete("DROP INDEX");
        break;
      case QueryType::QUERY_DROP_SCHEMA:
        WriteCommandComplete("DROP SCHEMA");
        break;
      case QueryType::QUERY_SET:
        WriteCommandComplete("SET");
        break;
      default:
        WriteCommandComplete("This QueryType needs a completion message!");
        break;
    }
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

  /**
   * Write a data row from the execution engine back to the client
   * @param tuple pointer to the start of the row
   * @param columns OutputSchema describing the tuple
   */
  void WriteDataRow(const byte *const tuple, const std::vector<planner::OutputSchema::Column> &columns) {
    if (format_ == FieldFormat::text) {
      WriteTextDataRow(tuple, columns);
      return;
    }
    WriteBinaryDataRow(tuple, columns);
  }

 private:
  // Maybe make this mutable in the future if we want to reuse these objects? Otherwise since it seems like its life
  // cycle is tied to a query, we only need to instantiate the value once.
  const FieldFormat format_ = FieldFormat::text;

  /**
   * Write a data row in Postgres' binary format coming from an OutputBuffer in the execution engine. Unclear when the
   * server should use this just yet.
   * @param tuple pointer to the start of the row
   * @param columns OutputSchema describing the tuple
   */
  void WriteBinaryDataRow(const byte *const tuple, const std::vector<planner::OutputSchema::Column> &columns) {
    BeginPacket(NetworkMessageType::PG_DATA_ROW).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
    uint32_t curr_offset = 0;
    for (const auto &col : columns) {
      // Reinterpret to a base value type first and check if it's NULL
      const auto *const val = reinterpret_cast<const execution::sql::Val *const>(tuple + curr_offset);

      if (val->is_null_) {
        // write a -1 for the length of the column value and continue to the next value
        AppendValue<int32_t>(static_cast<int32_t>(-1));
        continue;
      }

      const auto type_size = execution::sql::ValUtil::GetSqlSize(col.GetType());

      // Write the attribute
      switch (col.GetType()) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::BIGINT:
        case type::TypeId::INTEGER: {
          auto *int_val = reinterpret_cast<const execution::sql::Integer *const>(val);
          AppendValue<int32_t>(static_cast<int32_t>(type_size)).AppendValue<int64_t>(int_val->val_);
          break;
        }
        case type::TypeId::BOOLEAN: {
          auto *bool_val = reinterpret_cast<const execution::sql::BoolVal *const>(val);
          AppendValue<int32_t>(static_cast<int32_t>(type_size)).AppendValue<bool>(bool_val->val_);
          break;
        }
        case type::TypeId::DECIMAL: {
          auto *real_val = reinterpret_cast<const execution::sql::Real *const>(val);
          AppendValue<int32_t>(static_cast<int32_t>(type_size)).AppendValue<double>(real_val->val_);
          break;
        }
        case type::TypeId::DATE: {
          auto *date_val = reinterpret_cast<const execution::sql::Date *const>(val);
          // TODO(Matt): would we ever use the ymd_ format for the wire?
          AppendValue<int32_t>(static_cast<int32_t>(type_size)).AppendValue<uint32_t>(date_val->int_val_);
          break;
        }
        case type::TypeId::VARCHAR: {
          auto *string_val = reinterpret_cast<const execution::sql::StringVal *const>(val);
          AppendValue<int32_t>(static_cast<int32_t>(string_val->len_))
              .AppendStringView(string_val->StringView(), false);
          break;
        }
        default:
          UNREACHABLE("Cannot output unsupported type!!!");
      }
      // Advance in the buffer based on the execution engine's type size
      curr_offset += type_size;
    }
    EndPacket();
  }

  /**
   * Write a data row in Postgres' text format coming from an OutputBuffer in the execution engine. Simple Query
   * messages always reply with text format data.
   * @param tuple pointer to the start of the row
   * @param columns OutputSchema describing the tuple
   */
  void WriteTextDataRow(const byte *const tuple, const std::vector<planner::OutputSchema::Column> &columns) {
    BeginPacket(NetworkMessageType::PG_DATA_ROW).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
    uint32_t curr_offset = 0;
    for (const auto &col : columns) {
      // Reinterpret to a base value type first and check if it's NULL
      const auto *const val = reinterpret_cast<const execution::sql::Val *const>(tuple + curr_offset);

      if (val->is_null_) {
        // write a -1 for the length of the column value and continue to the next value
        AppendValue<int32_t>(static_cast<int32_t>(-1));
        continue;
      }

      // Convert the field to text format
      std::string string_value;
      switch (col.GetType()) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::BIGINT:
        case type::TypeId::INTEGER: {
          auto *int_val = reinterpret_cast<const execution::sql::Integer *const>(val);
          string_value = std::to_string(int_val->val_);
          break;
        }
        case type::TypeId::BOOLEAN: {
          auto *bool_val = reinterpret_cast<const execution::sql::BoolVal *const>(val);
          string_value = (static_cast<bool>(bool_val->val_) ? POSTGRES_BOOLEAN_STR_TRUE : POSTGRES_BOOLEAN_STR_FALSE);
          break;
        }
        case type::TypeId::DECIMAL: {
          auto *real_val = reinterpret_cast<const execution::sql::Real *const>(val);
          string_value = std::to_string(real_val->val_);
          break;
        }
        case type::TypeId::DATE: {
          auto *date_val = reinterpret_cast<const execution::sql::Date *const>(val);
          // TODO(Matt): would we ever use the ymd_ format for the wire?
          string_value = std::to_string(date_val->int_val_);
          break;
        }
        case type::TypeId::VARCHAR: {
          // Don't allocate an actual string for a VARCHAR, just wrap a std::string_view, write the value directly, and
          // continue
          auto *string_val = reinterpret_cast<const execution::sql::StringVal *const>(val);
          AppendValue<int32_t>(static_cast<int32_t>(string_val->len_))
              .AppendRaw(string_val->Content(), string_val->len_);
          curr_offset += execution::sql::ValUtil::GetSqlSize(col.GetType());
          continue;
        }
        default:
          UNREACHABLE("Cannot output unsupported type!!!");
      }

      AppendValue<int32_t>(static_cast<int32_t>(string_value.length())).AppendString(string_value, false);

      // Advance in the buffer based on the execution engine's type size
      curr_offset += execution::sql::ValUtil::GetSqlSize(col.GetType());
    }
    EndPacket();
  }
};

}  // namespace terrier::network
