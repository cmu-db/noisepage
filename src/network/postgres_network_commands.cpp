#include "network/postgres_network_commands.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "parser/postgresparser.h"

namespace terrier {
namespace network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.


Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                    PostgresPacketWriter &out,
                                    CallbackFunc callback) {
  interpreter.protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  std::string query = in_.ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());
  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              CallbackFunc) {

  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition BindCommand::Exec(PostgresProtocolInterpreter &interpreter,
                             PostgresPacketWriter &out,
                             CallbackFunc) {
  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                 PostgresPacketWriter &out,
                                 CallbackFunc) {
  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                PostgresPacketWriter &out,
                                CallbackFunc callback) {
  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter &interpreter,
                             PostgresPacketWriter &out,
                             CallbackFunc) {
  out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                           "The prepared statement does not exist"}});
  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              CallbackFunc) {
  // Send close complete response
  out.WriteSingleTypePacket(NetworkMessageType::CLOSE_COMPLETE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter &,
                                  PostgresPacketWriter &,
                                  CallbackFunc) {
  LOG_INFO("Terminated");
  return Transition::TERMINATE;
}
} // namespace network
} // namespace terrier
