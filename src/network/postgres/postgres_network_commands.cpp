#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>

#include "network/postgres/postgres_protocol_interpreter.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "traffic_cop/traffic_cop.h"
#include "traffic_cop/traffic_cop_util.h"

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

static void ExecuteStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                             const common::ManagedPointer<network::PostgresPacketWriter> out,
                             const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             const common::ManagedPointer<parser::ParseResult> parse_result,
                             const terrier::network::QueryType query_type) {
  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (query_type <= network::QueryType::QUERY_ROLLBACK) {
    t_cop->ExecuteTransactionStatement(connection_ctx, out, query_type);
    return;
  }

  if (query_type >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    // TODO(Matt): add a TRAFFIC_COP_LOG_INFO here
    out->WriteCommandComplete(query_type, 0);
    return;
  }

  const bool single_statement_txn = connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE;

  // Begin a transaction if necessary
  if (single_statement_txn) {
    t_cop->BeginTransaction(connection_ctx);
  }

  // Try to bind the parsed statement
  if (t_cop->BindStatement(connection_ctx, out, parse_result, query_type)) {
    // Binding succeeded, optimize to generate a physical plan and then execute
    auto physical_plan =
        t_cop->OptimizeBoundQuery(connection_ctx->Transaction(), connection_ctx->Accessor(), parse_result);

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (query_type <= network::QueryType::QUERY_DELETE) {
      // DML query to put through codegen
      t_cop->CodegenAndRunPhysicalPlan(connection_ctx, out, common::ManagedPointer(physical_plan), query_type);
    } else if (query_type <= network::QueryType::QUERY_CREATE_VIEW) {
      t_cop->ExecuteCreateStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                                    single_statement_txn);
    } else if (query_type <= network::QueryType::QUERY_DROP_VIEW) {
      t_cop->ExecuteDropStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                                  single_statement_txn);
    }
  }

  if (single_statement_txn) {
    // Single statement transaction should be ended before returning
    // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
    t_cop->EndTransaction(connection_ctx, connection_ctx->Transaction()->MustAbort()
                                              ? network::QueryType::QUERY_ROLLBACK
                                              : network::QueryType::QUERY_COMMIT);
  }
}

Transition SimpleQueryCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                    const common::ManagedPointer<PostgresPacketWriter> out,
                                    const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                    const common::ManagedPointer<ConnectionContext> connection) {
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
  const auto query_type = trafficcop::TrafficCopUtil::QueryTypeForStatement(statement);

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      query_type != QueryType::QUERY_COMMIT && query_type != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return FinishSimpleQueryCommand(out, connection);
  }

  // Pass the statement to be executed by the traffic cop
  ExecuteStatement(connection, out, t_cop, common::ManagedPointer(parse_result), query_type);

  return FinishSimpleQueryCommand(out, connection);
}

Transition ParseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  std::string stmt_name = in_.ReadString();
  NETWORK_LOG_TRACE("ParseCommand Statement Name: {0}", stmt_name.c_str());

  std::string query = in_.ReadString();
  NETWORK_LOG_INFO("ParseCommand: {0}", query);

  const auto num_params = in_.ReadValue<int16_t>();
  std::vector<PostgresValueType> param_types;
  param_types.reserve(num_params);
  for (uint16_t i = 0; i < num_params; i++) {
    param_types.emplace_back(in_.ReadValue<PostgresValueType>());
  }

  const auto parse_result = t_cop->ParseQuery(query, connection, out);

  out->WriteParseComplete();
  return Transition::PROCEED;
}

Transition BindCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                             const common::ManagedPointer<PostgresPacketWriter> out,
                             const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             const common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Bind Command");
  out->WriteBindComplete();
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 const common::ManagedPointer<PostgresPacketWriter> out,
                                 const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 const common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  out->WriteNoData();
  NETWORK_LOG_TRACE("Describe Command");
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                const common::ManagedPointer<PostgresPacketWriter> out,
                                const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                const common::ManagedPointer<ConnectionContext> connection) {
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

Transition CloseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Matt): Implement this for prepared statement support
  NETWORK_LOG_TRACE("Close Command");
  // Send close complete response
  out->WriteCloseCommand(DescribeCommandObjectType::PORTAL, "");
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                  const common::ManagedPointer<PostgresPacketWriter> out,
                                  const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                  const common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Terminated");
  // Postgres doesn't send any sort of response for the Terminate command
  // We don't do removal of the temp namespace at the Command level because it's possible that we don't receive a
  // Terminate packet to generate the Command from, and instead closed the connection due to timeout
  return Transition::TERMINATE;
}

Transition EmptyCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Empty Command");
  out->WriteEmptyQueryResponse();
  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  return Transition::PROCEED;
}
}  // namespace terrier::network
