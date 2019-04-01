#include <memory>
#include <string>
#include <utility>
#include "type/transient_value_factory.h"
#include "network/postgres_network_commands.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "traffic_cop/traffic_cop.h"

namespace terrier::network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.

void PostgresNetworkCommand::AcceptResults(const traffic_cop::ResultSet &result_set, PostgresPacketWriter *const out) {
  if (result_set.column_names_.empty()) {
    out->WriteEmptyQueryResponse();
    return;
  }

  out->WriteRowDescription(result_set.column_names_);
  for (auto &row : result_set.rows_) out->WriteDataRow(row);

  // TODO(Weichen): We need somehow to know which kind of query it is. (INSERT? DELETE? SELECT?)
  // and the number of rows. This is needed in the tag. Now we just use an empty string.

  out->WriteCommandComplete("");
}

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter *interpreter,
                                    PostgresPacketWriter *out,
                                    TrafficCopPtr t_cop,
                                    ConnectionContext *connection,
                                    NetworkCallback callback) {
  interpreter->protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute query: {0}", query.c_str());

  SimpleQueryCallback result_callback = AcceptResults;

  t_cop->ExecuteQuery(query.c_str(), out, result_callback);
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter *interpreter,
                              PostgresPacketWriter *out,
                              TrafficCopPtr t_cop,
                              ConnectionContext *connection,
                              NetworkCallback callback) {

  std::string stmt_name = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query Statement Name: {0}", stmt_name.c_str());

  std::string query = in_.ReadString();
  auto num_params = in_.ReadValue<uint16_t>();
  std::vector<type::TypeId> param_types(num_params);
  for(auto &param_type : param_types)
  {
    auto oid = in_.ReadValue<int32_t>();
    param_type = PostgresValueTypeToInternalValueType(static_cast<PostgresValueType>(oid));
  }

  connection->statements.emplace(stmt_name, t_cop->Parse(query.c_str(), param_types));

  out->WriteParseComplete();
  return Transition::PROCEED;
}



Transition BindCommand::Exec(PostgresProtocolInterpreter *interpreter,
                             PostgresPacketWriter *out,
                             TrafficCopPtr t_cop,
                             ConnectionContext *connection,
                             NetworkCallback callback) {
  std::string portal_name = in_.ReadString();

  std::string stmt_name = in_.ReadString();
  auto statement_pair = connection->statements.find(stmt_name);
  if(statement_pair == connection->statements.end()){
    NETWORK_LOG_ERROR("Error: There is no statement with name {0}", stmt_name);
    throw NETWORK_PROCESS_EXCEPTION("");
  }

  // Find out param formats
  traffic_cop::Statement *statement = &statement_pair->second;
  auto num_formats = static_cast<size_t>(in_.ReadValue<int16_t>());
  std::vector<int16_t> is_binary;
  size_t num_params = statement->NumParams();
  if(num_formats == 0)
  {
    is_binary = std::vector<int16_t>(num_params, 0);
  }
  else if(num_formats == 1)
  {
    auto format = in_.ReadValue<int16_t>();
    is_binary = std::vector<int16_t>(num_params, format);
  }
  else if(num_formats == num_params)
  {
    for(size_t i = 0; i < num_formats; i++)
    {
      auto format = in_.ReadValue<int16_t>();
      is_binary.push_back(format);
    }
  }
  else
  {
    NETWORK_LOG_ERROR("Error: Numbers of parameters don't match. {0} in statement, (1) in format code.", num_params, num_formats);
    throw NETWORK_PROCESS_EXCEPTION("");
  }


  // Read param values
  auto num_params_from_query = static_cast<size_t>(in_.ReadValue<int16_t>());
  if(num_params_from_query != num_params)
  {
    NETWORK_LOG_ERROR("Error: Numbers of parameters don't match. {0} in statement, {1} in bind command", num_params, num_params_from_query);
    throw NETWORK_PROCESS_EXCEPTION("");
  }

  using namespace type;
  std::vector<TransientValue> params;

  for(size_t i=0; i<num_params; i++)
  {
    auto len = static_cast<size_t>(in_.ReadValue<int32_t>());

    if(statement->param_types[i] == TypeId::INTEGER)
    {
      int32_t value;
      if(is_binary[i] == 0)
      {
        char buf[len];
        in_.Read(len, buf);
        value = std::stoi(buf);
      }
      else
      {
        value = in_.ReadValue<int32_t>();
      }

      params.push_back(TransientValueFactory::GetInteger(value));
    }


  }


  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter *interpreter,
                                 PostgresPacketWriter *out,
                                 TrafficCopPtr t_cop,
                                 ConnectionContext *connection,
                                 NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter *interpreter,
                                PostgresPacketWriter *out,
                                TrafficCopPtr t_cop,
                                ConnectionContext *connection,
                                NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Exec query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter *interpreter,
                             PostgresPacketWriter *out,
                             TrafficCopPtr t_cop,
                             ConnectionContext *connection,
                             NetworkCallback callback) {
  NETWORK_LOG_TRACE("Sync query");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter *interpreter,
                              PostgresPacketWriter *out,
                              TrafficCopPtr t_cop,
                              ConnectionContext *connection,
                              NetworkCallback callback) {
  // Send close complete response
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter *interpreter,
                                  PostgresPacketWriter *out,
                                  TrafficCopPtr t_cop,
                                  ConnectionContext *connection,
                                  NetworkCallback callback) {
  NETWORK_LOG_TRACE("Terminated");
  return Transition::TERMINATE;
}
}  // namespace terrier::network
