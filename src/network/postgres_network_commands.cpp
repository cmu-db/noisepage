#include <memory>
#include <string>
#include <utility>

#include "network/postgres_network_commands.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "parser/postgresparser.h"

namespace terrier::network {

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                    CallbackFunc callback) {
  interpreter->protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              CallbackFunc callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition BindCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             CallbackFunc callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Bind query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                 CallbackFunc callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Parse query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                CallbackFunc callback) {
  std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Exec query: {0}", query.c_str());
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                             CallbackFunc callback) {
  NETWORK_LOG_TRACE("Sync query");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                              CallbackFunc callback) {
  // Send close complete response
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter *const interpreter, PostgresPacketWriter *const out,
                                  CallbackFunc callback) {
  NETWORK_LOG_TRACE("Terminated");
  return Transition::TERMINATE;
}
}  // namespace terrier::network
