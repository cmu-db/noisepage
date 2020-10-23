#include "network/postgres/postgres_packet_writer.h"

#include "common/error/error_data.h"
#include "execution/sql/value.h"
#include "network/postgres/postgres_defs.h"
#include "network/postgres/postgres_protocol_util.h"

namespace noisepage::network {

void PostgresPacketWriter::WriteReadyForQuery(NetworkTransactionStateType txn_status) {
  BeginPacket(NetworkMessageType::PG_READY_FOR_QUERY).AppendRawValue(txn_status).EndPacket();
}

void PostgresPacketWriter::WriteStartupResponse() {
  BeginPacket(NetworkMessageType::PG_AUTHENTICATION_REQUEST).AppendValue<int32_t>(0).EndPacket();

  for (auto &entry : PG_PARAMETER_STATUS_MAP)
    BeginPacket(NetworkMessageType::PG_PARAMETER_STATUS)
        .AppendString(entry.first, true)
        .AppendString(entry.second, true)
        .EndPacket();
  WriteReadyForQuery(NetworkTransactionStateType::IDLE);
}

void PostgresPacketWriter::WriteSimpleQuery(const std::string &query) {
  BeginPacket(NetworkMessageType::PG_SIMPLE_QUERY_COMMAND).AppendString(query, true).EndPacket();
}

void PostgresPacketWriter::WriteError(const common::ErrorData &error) {
  if (error.GetSeverity() <= common::ErrorSeverity::PANIC)
    BeginPacket(NetworkMessageType::PG_ERROR_RESPONSE);
  else
    BeginPacket(NetworkMessageType::PG_NOTICE_RESPONSE);

  AppendRawValue(common::ErrorField::HUMAN_READABLE_ERROR).AppendString(error.GetMessage(), true);
  AppendRawValue(common::ErrorField::CODE).AppendStringView(common::ErrorCodeToString(error.GetCode()), true);

  for (const auto &field : error.Fields()) {
    AppendRawValue(field.first).AppendString(field.second, true);
  }

  AppendRawValue<uchar>(0).EndPacket();  // Nul-terminate packet
}

void PostgresPacketWriter::WriteEmptyQueryResponse() {
  BeginPacket(NetworkMessageType::PG_EMPTY_QUERY_RESPONSE).EndPacket();
}

void PostgresPacketWriter::WriteNoData() { BeginPacket(NetworkMessageType::PG_NO_DATA_RESPONSE).EndPacket(); }

void PostgresPacketWriter::WriteParameterDescription(const std::vector<type::TypeId> &param_types) {
  BeginPacket(NetworkMessageType::PG_PARAMETER_DESCRIPTION);
  AppendValue<int16_t>(static_cast<int16_t>(param_types.size()));

  for (auto &type : param_types)
    AppendValue<int32_t>(static_cast<int32_t>(PostgresProtocolUtil::InternalValueTypeToPostgresValueType(type)));

  EndPacket();
}

void PostgresPacketWriter::WriteRowDescription(const std::vector<planner::OutputSchema::Column> &columns,
                                               const std::vector<FieldFormat> &field_formats) {
  BeginPacket(NetworkMessageType::PG_ROW_DESCRIPTION).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));

  for (uint32_t i = 0; i < columns.size(); i++) {
    const auto col_type = columns[i].GetType();

    NOISEPAGE_ASSERT(field_formats.size() == columns.size() || field_formats.size() == 1,
                     "Field formats can either be the size of the number of columns, or size 1 where they all use the "
                     "same format");
    const auto field_format = field_formats[i < field_formats.size() ? i : 0];

    // TODO(Matt): Figure out how to get table oid and column oids in the OutputSchema (Optimizer's job?)
    const auto &name =
        columns[i].GetExpr()->GetAlias().empty() ? columns[i].GetName() : columns[i].GetExpr()->GetAlias();
    // If a column has no name, then Postgres will return "?column?" as a column name.

    if (name.empty())
      AppendStringView("?column?", true);
    else
      AppendString(name, true);
    AppendValue<int32_t>(
        0)  // FIXME(Matt): We need this info in OutputSchema. Should be table oid (if it's a column from a table), 0
            // otherwise.
        .AppendValue<int16_t>(0)  // FIXME(Matt): We need this info in OutputSchema. Should be column oid (if
                                  // it's a column from a table), 0 otherwise.
        .AppendValue(
            static_cast<int32_t>(PostgresProtocolUtil::InternalValueTypeToPostgresValueType(col_type)));  // type oid
    if (col_type == type::TypeId::VARCHAR || col_type == type::TypeId::VARBINARY ||
        (field_format == FieldFormat::text && static_cast<uint8_t>(col_type) > 5)) {
      AppendValue<int16_t>(-1);  // variable length
    } else {
      AppendValue<int16_t>(type::TypeUtil::GetTypeSize(col_type));  // data type size
    }

    AppendValue<int32_t>(-1)  // type modifier, generally -1 (see pg_attribute.atttypmod)
        .AppendValue<int16_t>(
            static_cast<int16_t>(field_format));  // format code for the field, 0 for text, 1 for binary
  }
  EndPacket();
}

void PostgresPacketWriter::WriteCommandComplete(const std::string_view tag) {
  BeginPacket(NetworkMessageType::PG_COMMAND_COMPLETE).AppendStringView(tag, true).EndPacket();
}

void PostgresPacketWriter::WriteCommandComplete(const std::string_view tag, const uint32_t num_rows) {
  BeginPacket(NetworkMessageType::PG_COMMAND_COMPLETE)
      .AppendStringView(tag, false)
      .AppendString(std::to_string(num_rows), true)
      .EndPacket();
}

void PostgresPacketWriter::WriteCommandComplete(const QueryType query_type, const uint32_t num_rows) {
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
      WriteCommandComplete("INSERT 0 ", num_rows);
      break;
    case QueryType::QUERY_DELETE:
      WriteCommandComplete("DELETE ", num_rows);
      break;
    case QueryType::QUERY_UPDATE:
      WriteCommandComplete("UPDATE ", num_rows);
      break;
    case QueryType::QUERY_SELECT:
      WriteCommandComplete("SELECT ", num_rows);
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

void PostgresPacketWriter::WriteParseCommand(const std::string &destinationStmt, const std::string &query,
                                             const std::vector<int32_t> &params) {
  PacketWriter &writer = BeginPacket(NetworkMessageType::PG_PARSE_COMMAND)
                             .AppendString(destinationStmt, true)
                             .AppendString(query, true)
                             .AppendValue(static_cast<int16_t>(params.size()));
  for (auto param : params) {
    writer.AppendValue(param);
  }
  writer.EndPacket();
}

void PostgresPacketWriter::WriteBindCommand(const std::string &destinationPortal, const std::string &sourcePreparedStmt,
                                            std::initializer_list<int16_t> paramFormatCodes,
                                            std::initializer_list<std::vector<char> *> paramVals,
                                            std::initializer_list<int16_t> resultFormatCodes) {
  PacketWriter &writer = BeginPacket(NetworkMessageType::PG_BIND_COMMAND)
                             .AppendString(destinationPortal, true)
                             .AppendString(sourcePreparedStmt, true);
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

void PostgresPacketWriter::WriteExecuteCommand(const std::string &portal, int32_t rowLimit) {
  BeginPacket(NetworkMessageType::PG_EXECUTE_COMMAND).AppendString(portal, true).AppendValue(rowLimit).EndPacket();
}

void PostgresPacketWriter::WriteSyncCommand() { BeginPacket(NetworkMessageType::PG_SYNC_COMMAND).EndPacket(); }

void PostgresPacketWriter::WriteDescribeCommand(DescribeCommandObjectType type, const std::string &objectName) {
  BeginPacket(NetworkMessageType::PG_DESCRIBE_COMMAND).AppendRawValue(type).AppendString(objectName, true).EndPacket();
}

void PostgresPacketWriter::WriteCloseCommand(DescribeCommandObjectType type, const std::string &objectName) {
  BeginPacket(NetworkMessageType::PG_CLOSE_COMMAND).AppendRawValue(type).AppendString(objectName, true).EndPacket();
}

void PostgresPacketWriter::WriteParseComplete() { BeginPacket(NetworkMessageType::PG_PARSE_COMPLETE).EndPacket(); }

void PostgresPacketWriter::WriteCloseComplete() { BeginPacket(NetworkMessageType::PG_CLOSE_COMPLETE).EndPacket(); }

void PostgresPacketWriter::WriteBindComplete() { BeginPacket(NetworkMessageType::PG_BIND_COMPLETE).EndPacket(); }

void PostgresPacketWriter::WriteDataRow(const byte *const tuple,
                                        const std::vector<planner::OutputSchema::Column> &columns,
                                        const std::vector<FieldFormat> &field_formats) {
  BeginPacket(NetworkMessageType::PG_DATA_ROW).AppendValue<int16_t>(static_cast<int16_t>(columns.size()));
  uint32_t curr_offset = 0;
  for (uint32_t i = 0; i < columns.size(); i++) {
    // Reinterpret to a base value type first and check if it's NULL
    auto alignment = execution::sql::ValUtil::GetSqlAlignment(columns[i].GetType());
    if (!common::MathUtil::IsAligned(curr_offset, alignment)) {
      curr_offset = static_cast<uint32_t>(common::MathUtil::AlignTo(curr_offset, alignment));
    }
    const auto *const val = reinterpret_cast<const execution::sql::Val *const>(tuple + curr_offset);

    // Field formats can either be the size of the number of columns, or size 1 where they all use the same format
    const auto field_format = field_formats[i < field_formats.size() ? i : 0];

    const auto type_size = field_format == FieldFormat::text ? WriteTextAttribute(val, columns[i].GetType())
                                                             : WriteBinaryAttribute(val, columns[i].GetType());

    // Advance in the buffer based on the execution engine's type size
    curr_offset += type_size;
  }
  EndPacket();
}

template <class native_type, class val_type>
void PostgresPacketWriter::WriteBinaryVal(const execution::sql::Val *const val, const type::TypeId type) {
  const auto *const casted_val = reinterpret_cast<const val_type *const>(val);
  NOISEPAGE_ASSERT(type::TypeUtil::GetTypeSize(type) == sizeof(native_type),
                   "Mismatched native type size and size reported by TypeUtil.");
  // write the length, write the attribute
  AppendValue<int32_t>(static_cast<int32_t>(type::TypeUtil::GetTypeSize(type)))
      .AppendValue<native_type>(static_cast<native_type>(casted_val->val_));
}

template <class native_type, class val_type>
void PostgresPacketWriter::WriteBinaryValNeedsToNative(const execution::sql::Val *const val, const type::TypeId type) {
  const auto *const casted_val = reinterpret_cast<const val_type *const>(val);
  NOISEPAGE_ASSERT(type::TypeUtil::GetTypeSize(type) == sizeof(native_type),
                   "Mismatched native type size and size reported by TypeUtil.");
  // write the length, write the attribute
  AppendValue<int32_t>(static_cast<int32_t>(type::TypeUtil::GetTypeSize(type)))
      .AppendValue<native_type>(static_cast<native_type>(casted_val->val_.ToNative()));
}

uint32_t PostgresPacketWriter::WriteBinaryAttribute(const execution::sql::Val *const val, const type::TypeId type) {
  if (val->is_null_) {
    // write a -1 for the length of the column value and continue to the next value
    AppendValue<int32_t>(static_cast<int32_t>(-1));
  } else {
    // Write the attribute
    switch (type) {
      case type::TypeId::TINYINT: {
        WriteBinaryVal<int8_t, execution::sql::Integer>(val, type);
        break;
      }
      case type::TypeId::SMALLINT: {
        WriteBinaryVal<int16_t, execution::sql::Integer>(val, type);
        break;
      }
      case type::TypeId::INTEGER: {
        WriteBinaryVal<int32_t, execution::sql::Integer>(val, type);
        break;
      }
      case type::TypeId::BIGINT: {
        WriteBinaryVal<int64_t, execution::sql::Integer>(val, type);
        break;
      }
      case type::TypeId::BOOLEAN: {
        WriteBinaryVal<bool, execution::sql::BoolVal>(val, type);
        break;
      }
      case type::TypeId::DECIMAL: {
        WriteBinaryVal<double, execution::sql::Real>(val, type);
        break;
      }
      case type::TypeId::DATE: {
        WriteBinaryValNeedsToNative<uint32_t, execution::sql::DateVal>(val, type);
        break;
      }
      case type::TypeId::TIMESTAMP: {
        WriteBinaryValNeedsToNative<uint64_t, execution::sql::TimestampVal>(val, type);
        break;
      }
      default:
        UNREACHABLE(
            "Unsupported type for binary serialization. This is either a new type, or an oversight when reading JDBC "
            "source code.");
    }
  }

  // Advance in the buffer based on the execution engine's type size
  return execution::sql::ValUtil::GetSqlSize(type);
}

uint32_t PostgresPacketWriter::WriteTextAttribute(const execution::sql::Val *const val, const type::TypeId type) {
  if (val->is_null_) {
    // write a -1 for the length of the column value and continue to the next value
    AppendValue<int32_t>(static_cast<int32_t>(-1));
  } else {
    // Convert the field to text format
    std::string string_value;
    switch (type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::BIGINT:
      case type::TypeId::INTEGER: {
        auto *int_val = reinterpret_cast<const execution::sql::Integer *const>(val);
        string_value = std::to_string(int_val->val_);
        break;
      }
      case type::TypeId::BOOLEAN: {
        // Don't allocate an actual string for a BOOLEAN, just wrap a std::string_view, write the value directly, and
        // continue
        auto *bool_val = reinterpret_cast<const execution::sql::BoolVal *const>(val);
        const auto str_view =
            static_cast<bool>(bool_val->val_) ? POSTGRES_BOOLEAN_STR_TRUE : POSTGRES_BOOLEAN_STR_FALSE;
        AppendValue<int32_t>(static_cast<int32_t>(str_view.length())).AppendStringView(str_view, false);
        return execution::sql::ValUtil::GetSqlSize(type);
      }
      case type::TypeId::DECIMAL: {
        auto *real_val = reinterpret_cast<const execution::sql::Real *const>(val);
        string_value = std::to_string(real_val->val_);
        break;
      }
      case type::TypeId::DATE: {
        auto *date_val = reinterpret_cast<const execution::sql::DateVal *const>(val);
        string_value = date_val->val_.ToString();
        break;
      }
      case type::TypeId::TIMESTAMP: {
        auto *ts_val = reinterpret_cast<const execution::sql::TimestampVal *const>(val);
        string_value = ts_val->val_.ToString();
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        // Don't allocate an actual string for a VARCHAR, just wrap a std::string_view, write the value directly, and
        // continue
        const auto *const string_val = reinterpret_cast<const execution::sql::StringVal *const>(val);
        AppendValue<int32_t>(static_cast<int32_t>(string_val->GetLength()))
            .AppendStringView(string_val->StringView(), false);
        return execution::sql::ValUtil::GetSqlSize(type);
      }
      default:
        UNREACHABLE(
            "Unsupported type for text serialization. This is either a new type, or an oversight when reading JDBC "
            "source code.");
    }

    // write the size, write the attribute
    AppendValue<int32_t>(static_cast<int32_t>(string_value.length())).AppendString(string_value, false);
  }

  // Advance in the buffer based on the execution engine's type size
  return execution::sql::ValUtil::GetSqlSize(type);
}

}  // namespace noisepage::network
