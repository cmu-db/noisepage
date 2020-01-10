#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
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

  t_cop->ExecuteSimpleQuery(query, connection, out, callback);

  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  // TODO(Matt): Implement this for prepared statement support
  out->WriteParseComplete();
  return Transition::PROCEED;
}

Transition BindCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  // TODO(Matt): Implement this for prepared statement support
  out->WriteBindComplete();
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 common::ManagedPointer<PostgresPacketWriter> out,
                                 common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  // TODO(Matt): Implement this for prepared statement support
  out->WriteNoData();
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                common::ManagedPointer<PostgresPacketWriter> out,
                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  // TODO(Matt): Implement this
  out->WriteCommandComplete("");
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) {
  NETWORK_LOG_TRACE("Sync query");
  if (connection->Transaction() != nullptr) {
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
