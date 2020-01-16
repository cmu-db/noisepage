#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>

#include "network/postgres/postgres_protocol_interpreter.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::network {

/**
 * SimpleQuery always
 * @param out
 * @param connection
 * @return
 */
static Transition FinishSimpleQueryCommand(const common::ManagedPointer<PostgresPacketWriter> out,
                                           const common::ManagedPointer<ConnectionContext> connection) {
  out->WriteReadyForQuery(connection->TransactionState());
  return Transition::PROCEED;
}

Transition SimpleQueryCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                    common::ManagedPointer<PostgresPacketWriter> out,
                                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                    common::ManagedPointer<ConnectionContext> connection) {
  const std::string query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute SimpleQuery: {0}", query.c_str());

  const auto parse_result = t_cop->ParseQuery(query, connection, out);

  if (parse_result == nullptr) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return FinishSimpleQueryCommand(out, connection);
  }

  TERRIER_ASSERT(parse_result->GetStatements().size() <= 1,
                 "We currently expect one statement per string (psql and oltpbench).");

  // TODO(Matt:) some clients may send multiple statements in a single SimpleQuery packet/string. Handling that would
  // probably exist here, looping over all of the elements in the ParseResult. It's not clear to me how the binder would
  // handle that though since you pass the ParseResult in for binding. Maybe bind ParseResult once?

  // Empty queries get a special response in postgres and do not care if they're in a failed txn block
  if (parse_result->Empty()) {
    out->WriteEmptyQueryResponse();
    return FinishSimpleQueryCommand(out, connection);
  }

  // It parsed and we've got our single statement
  const auto statement = parse_result->GetStatement(0);
  const auto statement_type = statement->GetType();

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      (statement_type != parser::StatementType::TRANSACTION ||
       statement.CastManagedPointerTo<parser::TransactionStatement>()->GetTransactionType() ==
           parser::TransactionStatement::CommandType::kBegin)) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return FinishSimpleQueryCommand(out, connection);
  }

  t_cop->ExecuteStatement(connection, out, common::ManagedPointer(parse_result), parse_result->GetStatement(0),
                          statement_type);

  return FinishSimpleQueryCommand(out, connection);
}

Transition ParseCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Parse Command");
  out->WriteParseComplete();
  return Transition::PROCEED;
}

Transition BindCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Bind Command");
  out->WriteBindComplete();
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 common::ManagedPointer<PostgresPacketWriter> out,
                                 common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  out->WriteNoData();
  NETWORK_LOG_TRACE("Describe Command");
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                common::ManagedPointer<PostgresPacketWriter> out,
                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Exec Command");
  out->WriteCommandComplete("");
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Sync query");
  out->WriteReadyForQuery(connection->TransactionState());
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Close Command");
  // Send close complete response
  out->WriteCloseCommand(DescribeCommandObjectType::PORTAL, "");
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                  common::ManagedPointer<PostgresPacketWriter> out,
                                  common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                  common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Terminated");
  // Postgres doesn't send any sort of response for the Terminate command
  // We don't do removal of the temp namespace at the Command level because it's possible that we don't receive a
  // Terminate packet to generate the Command from, and instead closed the connection due to timeout
  return Transition::TERMINATE;
}

Transition EmptyCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                              common::ManagedPointer<PostgresPacketWriter> out,
                              common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Empty Command");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}
}  // namespace terrier::network
