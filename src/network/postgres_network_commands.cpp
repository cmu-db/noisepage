#include <memory>
#include <string>
#include <utility>

#include "network/postgres_network_commands.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "parser/postgresparser.h"

namespace terrier::network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                    CallbackFunc callback) {
  interpreter->protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  std::string query = in_.ReadString();
  LOG_INFO("Execute query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              CallbackFunc callback) {
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition BindCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             CallbackFunc callback) {
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                 CallbackFunc callback) {
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                CallbackFunc callback) {
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             CallbackFunc callback) {
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              CallbackFunc callback) {
  // Send close complete response
  out->WriteSingleTypePacket(NetworkMessageType::CLOSE_COMPLETE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                  CallbackFunc callback) {
  LOG_INFO("Terminated");
  return Transition::TERMINATE;
}
}  // namespace terrier::network
