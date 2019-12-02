#include "network/postgres/postgres_network_commands.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/traffic_cop.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::network {

void LogAndWriteErrorMsg(const std::string &msg, common::ManagedPointer<PostgresPacketWriter> out) {
  NETWORK_LOG_ERROR(msg);
  out->WriteSingleErrorResponse(NetworkMessageType::PG_HUMAN_READABLE_ERROR, msg);
}

Transition SimpleQueryCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                    common::ManagedPointer<PostgresPacketWriter> out,
                                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                    common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute SimpleQuery: {0}", query.c_str());

  trafficcop::SqliteEngine *execution_engine = t_cop->GetExecutionEngine();
  sqlite3_stmt *stmt = execution_engine->PrepareStatement(query);
  trafficcop::ResultSet result = execution_engine->Execute(stmt);
  if (result.column_names_.empty()) {
    out->WriteEmptyQueryResponse();
  } else {
    out->WriteRowDescription(result.column_names_);
    for (auto &row : result.rows_) out->WriteDataRow(row);

    // TODO(Weichen): We need somehow to know which kind of query it is. (INSERT? DELETE? SELECT?)
    // and the number of rows. This is needed in the tag. Now we just use an empty string.

    out->WriteCommandComplete("");
  }

  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  sqlite3_finalize(stmt);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  std::string stmt_name = in_.ReadString();
  NETWORK_LOG_TRACE("ParseCommand Statement Name: {0}", stmt_name.c_str());

  std::string query = in_.ReadString();
  NETWORK_LOG_INFO("ParseCommand: {0}", query);

  if (query.find("BEGIN") != std::string::npos) {
    connection->in_transaction_ = true;
  } else if (query.find("COMMIT") != std::string::npos || query.find("ROLLBACK") != std::string::npos) {
    connection->in_transaction_ = false;
  }

  // TODO(Weichen): This implementation does not strictly follow Postgres protocol.
  // This param num here is just the number of params that the client wants to pre-specify types for.
  // Should not use this as the number of parameters.
  // Should rely on the table schema to determine the parameter type.
  auto num_params = in_.ReadValue<uint16_t>();
  std::vector<PostgresValueType> param_types;
  for (uint16_t i = 0; i < num_params; i++) {
    auto oid = in_.ReadValue<int32_t>();
    param_types.push_back(static_cast<PostgresValueType>(oid));
  }

  // if a statement with that name exists, return error
  if (!stmt_name.empty() && connection->statements_.count(stmt_name) > 0) {
    LogAndWriteErrorMsg("There is already a statement with name " + stmt_name, out);
    return Transition::PROCEED;
  }

  trafficcop::SqliteEngine *execution_engine = t_cop->GetExecutionEngine();
  sqlite3_stmt *sqlite_stmt = execution_engine->PrepareStatement(query);

  trafficcop::Statement stmt(sqlite_stmt, param_types, query);
  connection->statements_[stmt_name] = stmt;

  out->WriteParseComplete();
  return Transition::PROCEED;
}

Transition BindCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  using std::pair;
  using std::string;
  using std::vector;

  string portal_name = in_.ReadString();

  string stmt_name = in_.ReadString();
  NETWORK_LOG_TRACE("BindCommand, portal name = {0}, stmt name = {1}", portal_name, stmt_name);

  auto statement_pair = connection->statements_.find(stmt_name);
  if (statement_pair == connection->statements_.end()) {
    string error_msg = fmt::format("Error: There is no statement with name {0}", stmt_name);
    LogAndWriteErrorMsg(error_msg, out);
    return Transition::PROCEED;
  }

  // Find out param formats (text/binary)
  // You can find detailed documentation at
  // https://www.postgresql.org/docs/9.3/protocol-message-formats.html

  trafficcop::Statement *statement = &statement_pair->second;
  auto num_formats = static_cast<size_t>(in_.ReadValue<int16_t>());
  vector<int16_t> is_binary;
  size_t num_params = statement->NumParams();
  if (num_formats == 0) {
    // All params are text
    is_binary = std::vector<int16_t>(num_params, 0);
  } else if (num_formats == 1) {
    // the following number determines the format for all params (0=text, 1=binary)
    auto format = in_.ReadValue<int16_t>();
    is_binary = std::vector<int16_t>(num_params, format);
  } else if (num_formats == num_params) {
    // a number for every param states its format (0=text, 1=binary)
    for (size_t i = 0; i < num_formats; i++) {
      auto format = in_.ReadValue<int16_t>();
      is_binary.push_back(format);
    }
  } else {
    string error_msg = fmt::format(
        "Error: Numbers of parameters don't match. "
        "{0} in statement, {1} in format code.",
        num_params, num_formats);

    LogAndWriteErrorMsg(error_msg, out);
    return Transition::PROCEED;
  }

  // Read param values
  auto num_params_from_query = static_cast<size_t>(in_.ReadValue<int16_t>());
  if (num_params_from_query != num_params) {
    string error_msg = fmt::format(
        "Error: Numbers of parameters don't match. "
        "{0} in statement, {1} in bind command. "
        "This could be a result that the Parse command required type inference, which we don't support yet.",
        num_params, num_params_from_query);

    LogAndWriteErrorMsg(error_msg, out);
    return Transition::PROCEED;
  }

  using type::TransientValue;
  using type::TransientValueFactory;
  using type::TypeId;

  auto params = std::make_unique<std::vector<TransientValue>>();

  for (size_t i = 0; i < num_params; i++) {
    auto len = static_cast<int32_t>(in_.ReadValue<int32_t>());
    auto type = PostgresValueTypeToInternalValueType(statement->param_types_[i]);

    if (type == TypeId::INTEGER) {
      if (len != -1) {
        int32_t value;
        if (is_binary[i] == 0) {
          char buf[len + 1];
          memset(buf, 0, len + 1);
          in_.Read(len, buf);
          value = std::stoi(buf);
        } else {
          value = in_.ReadValue<int32_t>();
        }
        params->push_back(TransientValueFactory::GetInteger(value));
      } else {
        params->push_back(TransientValueFactory::GetNull(TypeId::INTEGER));
      }
    } else if (type == TypeId::DECIMAL) {
      if (len != -1) {
        double value;
        if (is_binary[i] == 0) {
          char buf[len + 1];
          memset(buf, 0, len + 1);
          in_.Read(len, buf);
          value = std::stod(buf);
        } else {
          value = in_.ReadValue<double>();
        }
        params->push_back(TransientValueFactory::GetDecimal(value));
      } else {
        params->push_back(TransientValueFactory::GetNull(TypeId::DECIMAL));
      }
    } else if (type == TypeId::VARCHAR) {
      if (len != -1) {
        char buf[len + 1];
        memset(buf, 0, len + 1);
        in_.Read(len, buf);
        params->push_back(TransientValueFactory::GetVarChar(buf));
      } else {
        params->push_back(TransientValueFactory::GetNull(TypeId::VARCHAR));
      }
    } else if (type == TypeId::TIMESTAMP) {
      if (len != -1) {
        char buf[len + 1];
        memset(buf, 0, len + 1);
        in_.Read(len, buf);
        params->push_back(TransientValueFactory::GetVarChar(buf));
      } else {
        params->push_back(TransientValueFactory::GetNull(TypeId::VARCHAR));
      }
    } else if (type == TypeId::BIGINT) {
      if (len != -1) {
        int64_t value;
        if (is_binary[i] == 0) {
          char buf[len + 1];
          memset(buf, 0, len + 1);
          in_.Read(len, buf);
          value = std::stoll(buf);
        } else {
          value = in_.ReadValue<int64_t>();
        }
        params->push_back(TransientValueFactory::GetBigInt(value));
      } else {
        params->push_back(TransientValueFactory::GetNull(TypeId::BIGINT));
      }
    } else {
      string error_msg =
          fmt::format("Param type {0} is not implemented yet", static_cast<int>(statement->param_types_[i]));

      LogAndWriteErrorMsg(error_msg, out);
      return Transition::PROCEED;
    }
  }

  // TODO(Weichen): Deal with requested response format. (text/binary)
  // Now they are all text.

  // With SQLite backend, we only produce a list of param values as the portal,
  // because we cannot copy a sqlite3 statement.
  trafficcop::Portal portal;
  portal.statement_ = statement;
  portal.params_ = std::move(params);
  connection->portals_[portal_name] = std::move(portal);

  out->WriteBindComplete();
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 common::ManagedPointer<PostgresPacketWriter> out,
                                 common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  auto type = in_.ReadValue<DescribeCommandObjectType>();
  std::string name = in_.ReadString();
  NETWORK_LOG_TRACE("Describe query: type = {0}, name = {1}", static_cast<char>(type), name.c_str());
  std::vector<std::string> column_names;

  trafficcop::SqliteEngine *execution_engine = t_cop->GetExecutionEngine();

  if (type == DescribeCommandObjectType::STATEMENT) {
    auto p_statement = connection->statements_.find(name);
    if (p_statement == connection->statements_.end()) {
      std::string error_msg = fmt::format("There is no statement with name {0}", name);
      LogAndWriteErrorMsg(error_msg, out);
      return Transition::PROCEED;
    }
    trafficcop::Statement &statement = p_statement->second;
    out->WriteParameterDescription(statement.param_types_);

    if (statement.sqlite3_stmt_ != nullptr) column_names = execution_engine->DescribeColumns(statement.sqlite3_stmt_);

  } else if (type == DescribeCommandObjectType::PORTAL) {
    auto p_portal = connection->portals_.find(name);
    if (p_portal == connection->portals_.end()) {
      std::string error_msg = fmt::format("There is no portal with name {0}", name);
      LogAndWriteErrorMsg(error_msg, out);
      return Transition::PROCEED;
    }
    if (p_portal->second.statement_->sqlite3_stmt_ != nullptr)
      column_names = execution_engine->DescribeColumns(p_portal->second.statement_->sqlite3_stmt_);

  } else {
    std::string error_msg = fmt::format("Wrong type: {0}, should be either 'S' or 'P'.", static_cast<char>(type));
    LogAndWriteErrorMsg(error_msg, out);
    return Transition::PROCEED;
  }

  if (column_names.empty())
    out->WriteNoData();
  else
    out->WriteRowDescription(column_names);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                common::ManagedPointer<PostgresPacketWriter> out,
                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  using std::string;
  string portal_name = in_.ReadString();
  NETWORK_LOG_TRACE("ExecuteCommand portal name = {0}", portal_name);

  auto p_portal = connection->portals_.find(portal_name);
  if (p_portal == connection->portals_.end()) {
    string error_msg = fmt::format("Error: Portal {0} does not exist.", portal_name);
    NETWORK_LOG_ERROR(error_msg);
    out->WriteSingleErrorResponse(NetworkMessageType::PG_HUMAN_READABLE_ERROR, error_msg);
    return Transition::PROCEED;
  }

  trafficcop::Portal &portal = p_portal->second;

  trafficcop::SqliteEngine *execution_engine = t_cop->GetExecutionEngine();
  execution_engine->Bind(portal.statement_->sqlite3_stmt_,
                         common::ManagedPointer<std::vector<type::TransientValue>>(portal.params_));
  trafficcop::ResultSet result = execution_engine->Execute(portal.statement_->sqlite3_stmt_);
  int32_t rows_affected = execution_engine->GetAffected();
  for (const auto &row : result.rows_) {
    out->WriteDataRow(row);
  }

  string &query_string = portal.statement_->query_string_;
  if (query_string.find("INSERT") != string::npos) {
    // TODO(YUZE): OID
    out->WriteCommandComplete("INSERT 0 " + std::to_string(rows_affected));
  } else if (query_string.find("DELETE") != string::npos) {
    out->WriteCommandComplete("DELETE " + std::to_string(rows_affected));
  } else if (query_string.find("UPDATE") != string::npos) {
    out->WriteCommandComplete("UPDATE " + std::to_string(rows_affected));
  } else if (query_string.find("SELECT") != string::npos) {
    out->WriteCommandComplete("SELECT " + std::to_string(result.rows_.size()));
  } else if (query_string.find("BEGIN") != string::npos) {
    out->WriteCommandComplete("BEGIN");
  }
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Sync query");
  if (connection->in_transaction_) {
    out->WriteReadyForQuery(NetworkTransactionStateType::BLOCK);
  } else {
    out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  }
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Close Command");
  // Send close complete response
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                  common::ManagedPointer<PostgresPacketWriter> out,
                                  common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                  common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Terminated");
  return Transition::TERMINATE;
}

Transition EmptyCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Empty Command");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}
}  // namespace terrier::network
