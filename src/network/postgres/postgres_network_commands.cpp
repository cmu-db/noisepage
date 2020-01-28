#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>

#include "network/postgres/postgres_packet_util.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/postgres/statement.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "traffic_cop/traffic_cop.h"
#include "traffic_cop/traffic_cop_util.h"

namespace terrier::network {

static Transition FinishSimpleQueryCommand(const common::ManagedPointer<PostgresPacketWriter> out,
                                           const common::ManagedPointer<ConnectionContext> connection) {
  out->WriteReadyForQuery(connection->TransactionState());
  return Transition::PROCEED;
}

// TODO(Matt): refactor this signature
static void ExecutePortal(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                          const common::ManagedPointer<planner::AbstractPlanNode>(physical_plan),
                          const common::ManagedPointer<network::PostgresPacketWriter> out,
                          const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                          const terrier::network::QueryType query_type, const bool single_statement_txn) {
  trafficcop::TrafficCopResult result;
  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (query_type <= network::QueryType::QUERY_DELETE) {
    // DML query to put through codegen
    if (query_type == network::QueryType::QUERY_SELECT)
      out->WriteRowDescription(physical_plan->GetOutputSchema()->GetColumns());
    result = t_cop->CodegenAndRunPhysicalPlan(connection_ctx, out, common::ManagedPointer(physical_plan), query_type);
  } else if (query_type <= network::QueryType::QUERY_CREATE_VIEW) {
    if (!single_statement_txn && query_type == network::QueryType::QUERY_CREATE_DB) {
      out->WriteErrorResponse("ERROR:  CREATE DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }

    // Right now this executor handles writing its results, so we don't need the result. Unclear if that changes in the
    // future
    t_cop->ExecuteCreateStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                                  single_statement_txn);
  } else if (query_type <= network::QueryType::QUERY_DROP_VIEW) {
    if (!single_statement_txn && query_type == network::QueryType::QUERY_DROP_DB) {
      out->WriteErrorResponse("ERROR:  DROP DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }

    // Right now this executor handles writing its results, so we don't need the result. Unclear if that changes in the
    // future
    t_cop->ExecuteDropStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                                single_statement_txn);
  }

  if (result.type_ == trafficcop::ResultType::COMPLETE) {
    TERRIER_ASSERT(std::holds_alternative<uint32_t>(result.extra_), "We're expecting number of rows here.");
    out->WriteCommandComplete(query_type, std::get<uint32_t>(result.extra_));
  } else {
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::ERROR,
                   "Currently only expecting COMPLETE or ERROR from TrafficCop here.");
    TERRIER_ASSERT(std::holds_alternative<std::string>(result.extra_), "We're expecting a message here.");
    out->WriteErrorResponse(std::get<std::string>(result.extra_));
  }
}

Transition SimpleQueryCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                    const common::ManagedPointer<PostgresPacketWriter> out,
                                    const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                    const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto query = in_.ReadString();
  NETWORK_LOG_TRACE("Execute SimpleQuery: {0}", query.c_str());

  auto statement = std::make_unique<network::Statement>(t_cop->ParseQuery(query, connection, out));

  if (!statement->Valid()) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return FinishSimpleQueryCommand(out, connection);
  }

  postgres_interpreter->SetStatement("", std::move(statement));
  const auto unnamed_statement = postgres_interpreter->GetStatement("");

  // TODO(Matt:) Clients may send multiple statements in a single SimpleQuery packet/string. Handling that would
  // probably exist here, looping over all of the elements in the ParseResult. It's not clear to me how the binder would
  // handle that though since you pass the ParseResult in for binding. Maybe bind ParseResult once?

  // Empty queries get a special response in postgres and do not care if they're in a failed txn block
  if (unnamed_statement->Empty()) {
    out->WriteEmptyQueryResponse();
    return FinishSimpleQueryCommand(out, connection);
  }

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      unnamed_statement->QueryType() != QueryType::QUERY_COMMIT &&
      unnamed_statement->QueryType() != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return FinishSimpleQueryCommand(out, connection);
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (unnamed_statement->QueryType() <= network::QueryType::QUERY_ROLLBACK) {
    t_cop->ExecuteTransactionStatement(connection, out, unnamed_statement->QueryType());
    return FinishSimpleQueryCommand(out, connection);
  }

  if (unnamed_statement->QueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    // TODO(Matt): add a TRAFFIC_COP_LOG_INFO here
    out->WriteCommandComplete(unnamed_statement->QueryType(), 0);
    return FinishSimpleQueryCommand(out, connection);
  }

  if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
    postgres_interpreter->SetSingleStatementTransaction(true);
  }

  // Begin a transaction if necessary
  if (postgres_interpreter->SingleStatementTransaction()) {
    t_cop->BeginTransaction(connection);
    postgres_interpreter->SetImplicitTransaction(true);
  }

  // Try to bind the parsed statement
  // TODO(Matt): refactor this signature
  const bool bind_result =
      t_cop->BindQuery(connection, out, unnamed_statement->ParseResult(), unnamed_statement->QueryType());
  if (bind_result) {
    // Binding succeeded, optimize to generate a physical plan and then execute
    // TODO(Matt): refactor this signature
    const auto physical_plan =
        t_cop->OptimizeBoundQuery(connection->Transaction(), connection->Accessor(), unnamed_statement->ParseResult());

    // TODO(Matt): refactor this signature
    ExecutePortal(connection, common::ManagedPointer(physical_plan), out, t_cop, unnamed_statement->QueryType(),
                  postgres_interpreter->SingleStatementTransaction());
  }

  if (postgres_interpreter->SingleStatementTransaction()) {
    // Single statement transaction should be ended before returning
    // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
    t_cop->EndTransaction(connection, connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                             : network::QueryType::QUERY_COMMIT);
    postgres_interpreter->SetSingleStatementTransaction(false);
    postgres_interpreter->SetImplicitTransaction(false);
  }

  return FinishSimpleQueryCommand(out, connection);
}

Transition ParseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto statement_name = in_.ReadString();
  NETWORK_LOG_TRACE("ParseCommand Statement Name: {0}", statement_name.c_str());

  const auto query = in_.ReadString();
  NETWORK_LOG_INFO("ParseCommand: {0}", query);

  auto param_types = PostgresPacketUtil::ReadParamTypes(common::ManagedPointer(&in_));

  auto statement =
      std::make_unique<network::Statement>(t_cop->ParseQuery(query, connection, out), std::move(param_types));

  if (!statement->Valid()) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return Transition::PROCEED;
  }

  postgres_interpreter->SetStatement("", std::move(statement));

  return Transition::PROCEED;
}

Transition BindCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                             const common::ManagedPointer<PostgresPacketWriter> out,
                             const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             const common::ManagedPointer<ConnectionContext> connection) {
  NETWORK_LOG_TRACE("Bind Command");
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto portal_name = in_.ReadString();

  const auto statement_name = in_.ReadString();

  const auto statement = postgres_interpreter->GetStatement(statement_name);

  if (statement == nullptr) {
    out->WriteErrorResponse("ERROR:  binding failed");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to bind fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return Transition::PROCEED;
  }

  // read out the parameter formats
  const auto param_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&in_));
  // TODO(Matt): would like to assert that this is 0 (all text), 1 (all the same), or the number of output columns

  // read the params
  auto params =
      PostgresPacketUtil::ReadParameters(common::ManagedPointer(&in_), statement->ParamTypes(), param_formats);

  // read out the result formats
  auto result_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&in_));
  // TODO(Matt): would like to assert that this is 0 (all text), 1 (all the same), or the number of output columns but
  // we can't do that without an OutputSchema yet this early in the pipeline

  postgres_interpreter->SetPortal(portal_name,
                                  std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));

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
