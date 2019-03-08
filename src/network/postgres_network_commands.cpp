#include <memory>
#include <string>
#include <utility>

#include "network/postgres_network_commands.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"

#include "traffic_cop/traffic_cop.h"

namespace terrier::network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.

void PostgresNetworkCommand::AcceptResults(traffic_cop::FakeResultSet &result_set, PostgresPacketWriter *const out) {
  if(result_set.column_names_.empty())
  {
    out->WriteEmptyQueryResponse();
    return;
  }

  out->WriteRowDescription(result_set.column_names_);
  for(auto &row:result_set.rows_)
    out->WriteDataRow(row);

  // TODO(Weichen): We need somehow to know which kind of query it is. (INSERT? DELETE? SELECT?)
  // and the number of rows. This is needed in the tag. Now just use an empty string.
  out->WriteCommandComplete("");
}

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                    TrafficCopPtr t_cop, NetworkCallback callback) {
  interpreter->protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute query: {0}", query.c_str());

  std::function<void(traffic_cop::FakeResultSet &, PostgresPacketWriter *)> result_callback = AcceptResults;

  t_cop->ExecuteQuery(query.c_str(), out, result_callback);
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              TrafficCopPtr t_cop, NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition BindCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             TrafficCopPtr t_cop, NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Bind query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                 TrafficCopPtr t_cop, NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                TrafficCopPtr t_cop, NetworkCallback callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Exec query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             TrafficCopPtr t_cop, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Sync query");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              TrafficCopPtr t_cop, NetworkCallback callback) {
  // Send close complete response
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                  TrafficCopPtr t_cop, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Terminated");
  return Transition::TERMINATE;
}
}  // namespace terrier::network
