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

static void ExecutePortal(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                          const common::ManagedPointer<Portal> portal,
                          const common::ManagedPointer<network::PostgresPacketWriter> out,
                          const common::ManagedPointer<trafficcop::TrafficCop> t_cop, const bool simple_query,
                          const bool single_statement_txn) {
  trafficcop::TrafficCopResult result;

  const auto query_type = portal->Statement()->QueryType();
  const auto physical_plan = portal->PhysicalPlan();

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (query_type <= network::QueryType::QUERY_DELETE) {
    // DML query to put through codegen
    if (simple_query && query_type == network::QueryType::QUERY_SELECT) {
      out->WriteRowDescription(physical_plan->GetOutputSchema()->GetColumns(), portal->ResultFormats());
    }
    result = t_cop->CodegenAndRunPhysicalPlan(connection_ctx, out, portal);
  } else if (query_type <= network::QueryType::QUERY_CREATE_VIEW) {
    if (!single_statement_txn && query_type == network::QueryType::QUERY_CREATE_DB) {
      out->WriteErrorResponse("ERROR:  CREATE DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    result = t_cop->ExecuteCreateStatement(connection_ctx, physical_plan, query_type, single_statement_txn);
  } else if (query_type <= network::QueryType::QUERY_DROP_VIEW) {
    if (!single_statement_txn && query_type == network::QueryType::QUERY_DROP_DB) {
      out->WriteErrorResponse("ERROR:  DROP DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    result = t_cop->ExecuteDropStatement(connection_ctx, physical_plan, query_type, single_statement_txn);
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

  auto statement = std::make_unique<network::Statement>(t_cop->ParseQuery(query, connection));

  // TODO(Matt): clear the unnamed stuff

  if (!statement->Valid()) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return FinishSimpleQueryCommand(out, connection);
  }

  // TODO(Matt:) Clients may send multiple statements in a single SimpleQuery packet/string. Handling that would
  // probably exist here, looping over all of the elements in the ParseResult. It's not clear to me how the binder would
  // handle that though since you pass the ParseResult in for binding. Maybe bind ParseResult once?

  // Empty queries get a special response in postgres and do not care if they're in a failed txn block
  if (statement->Empty()) {
    out->WriteEmptyQueryResponse();
    return FinishSimpleQueryCommand(out, connection);
  }

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      statement->QueryType() != QueryType::QUERY_COMMIT && statement->QueryType() != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return FinishSimpleQueryCommand(out, connection);
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (statement->QueryType() <= network::QueryType::QUERY_ROLLBACK) {
    t_cop->ExecuteTransactionStatement(connection, out, statement->QueryType());
    return FinishSimpleQueryCommand(out, connection);
  }

  if (statement->QueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
    out->WriteCommandComplete(statement->QueryType(), 0);
    return FinishSimpleQueryCommand(out, connection);
  }

  // Begin a transaction if necessary
  if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
    t_cop->BeginTransaction(connection);
    postgres_interpreter->SetSingleStatementTransaction(true);
    postgres_interpreter->SetImplicitTransaction(true);
  }

  // Try to bind the parsed statement
  // TODO(Matt): refactor this signature
  const auto bind_result = t_cop->BindQuery(connection, common::ManagedPointer(statement));
  if (bind_result.type_ == trafficcop::ResultType::COMPLETE) {
    // Binding succeeded, optimize to generate a physical plan and then execute
    auto physical_plan =
        t_cop->OptimizeBoundQuery(connection->Transaction(), connection->Accessor(), statement->ParseResult());

    const auto portal = std::make_unique<Portal>(common::ManagedPointer(statement), std::move(physical_plan));

    ExecutePortal(connection, common::ManagedPointer(portal), out, t_cop, true,
                  postgres_interpreter->SingleStatementTransaction());
  } else if (bind_result.type_ == trafficcop::ResultType::NOTICE) {
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    out->WriteNoticeResponse(std::get<std::string>(bind_result.extra_));
    out->WriteCommandComplete(statement->QueryType(), 0);
  } else {
    TERRIER_ASSERT(bind_result.type_ == trafficcop::ResultType::ERROR,
                   "I don't think we expect any other ResultType at this point.");
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    // failing to bind fails a transaction in postgres
    connection->Transaction()->SetMustAbort();
    out->WriteErrorResponse(std::get<std::string>(bind_result.extra_));
  }

  if (postgres_interpreter->SingleStatementTransaction()) {
    // Single statement transaction should be ended before returning
    // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
    t_cop->EndTransaction(connection, connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                             : network::QueryType::QUERY_COMMIT);
    postgres_interpreter->ResetTransactionState();
  }

  return FinishSimpleQueryCommand(out, connection);
}

Transition ParseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto statement_name = in_.ReadString();

  if (!statement_name.empty() && postgres_interpreter->GetStatement(statement_name) != nullptr) {
    out->WriteErrorResponse(
        "ERROR:  Named prepared statements must be explicitly closed before they can be redefined by another Parse "
        "message.");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return Transition::PROCEED;
  }

  const auto query = in_.ReadString();

  auto param_types = PostgresPacketUtil::ReadParamTypes(common::ManagedPointer(&in_));

  auto statement = std::make_unique<network::Statement>(t_cop->ParseQuery(query, connection), std::move(param_types));

  if (!statement->Valid()) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    postgres_interpreter->SetWaitingForSync(true);
    return Transition::PROCEED;
  }

  if (statement->QueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
  }

  postgres_interpreter->SetStatement(statement_name, std::move(statement));

  out->WriteParseComplete();

  return Transition::PROCEED;
}

Transition BindCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                             const common::ManagedPointer<PostgresPacketWriter> out,
                             const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto portal_name = in_.ReadString();
  const auto statement_name = in_.ReadString();
  const auto statement = postgres_interpreter->GetStatement(statement_name);

  if (statement == nullptr) {
    out->WriteErrorResponse("ERROR:  Statement name referenced by Bind message does not exist.");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to bind fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return Transition::PROCEED;
  }

  if (!portal_name.empty() && postgres_interpreter->GetPortal(statement_name) != nullptr) {
    out->WriteErrorResponse(
        "ERROR:  Named portals must be explicitly closed before they can be redefined by another Bind message.");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    return Transition::PROCEED;
  }

  // read out the parameter formats
  const auto param_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&in_));
  TERRIER_ASSERT(param_formats.size() == 1 || param_formats.size() == statement->ParamTypes().size(),
                 "Incorrect number of parameter format codes. Should either be 1 (all the same) or the number of "
                 "parameters required for this statement.");

  // read the params
  auto params =
      PostgresPacketUtil::ReadParameters(common::ManagedPointer(&in_), statement->ParamTypes(), param_formats);

  // read out the result formats
  auto result_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&in_));
  // TODO(Matt): would like to assert that this is 0 (all text), 1 (all the same), or the number of output columns but
  // we can't do that without an OutputSchema yet this early in the pipeline

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      statement->QueryType() != QueryType::QUERY_COMMIT && statement->QueryType() != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return Transition::PROCEED;
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (statement->QueryType() <= network::QueryType::QUERY_ROLLBACK) {
    // Don't begin an implicit txn in this case, and don't bind or optimize this statement
    postgres_interpreter->SetPortal(
        portal_name, std::make_unique<Portal>(statement, nullptr, std::move(params), std::move(result_formats)));
    out->WriteBindComplete();
    return Transition::PROCEED;
  }

  if (statement->QueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    // Don't begin an implicit txn in this case, and don't bind or optimize this statement
    postgres_interpreter->SetPortal(
        portal_name, std::make_unique<Portal>(statement, nullptr, std::move(params), std::move(result_formats)));
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
    out->WriteBindComplete();
    return Transition::PROCEED;
  }

  // Begin a transaction if necessary
  if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
    t_cop->BeginTransaction(connection);
    postgres_interpreter->SetImplicitTransaction(true);
  }

  // Bind it, plan it
  const auto bind_result = t_cop->BindQuery(connection, statement);
  if (bind_result.type_ == trafficcop::ResultType::COMPLETE) {
    // Binding succeeded, optimize to generate a physical plan and then execute
    auto physical_plan =
        t_cop->OptimizeBoundQuery(connection->Transaction(), connection->Accessor(), statement->ParseResult());

    postgres_interpreter->SetPortal(
        portal_name,
        std::make_unique<Portal>(statement, std::move(physical_plan), std::move(params), std::move(result_formats)));
    out->WriteBindComplete();
  } else if (bind_result.type_ == trafficcop::ResultType::NOTICE) {
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    postgres_interpreter->SetPortal(
        portal_name, std::make_unique<Portal>(statement, nullptr, std::move(params), std::move(result_formats)));
    out->WriteNoticeResponse(std::get<std::string>(bind_result.extra_));
    out->WriteBindComplete();
  } else {
    TERRIER_ASSERT(bind_result.type_ == trafficcop::ResultType::ERROR,
                   "I don't think we expect any other ResultType at this point.");
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    // failing to bind fails a transaction in postgres
    connection->Transaction()->SetMustAbort();
    out->WriteErrorResponse(std::get<std::string>(bind_result.extra_));
  }

  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 const common::ManagedPointer<PostgresPacketWriter> out,
                                 const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto object_type = in_.ReadValue<DescribeCommandObjectType>();
  const auto object_name = in_.ReadString();

  if (object_type == DescribeCommandObjectType::PORTAL) {
    const auto portal = postgres_interpreter->GetPortal(object_name);
    if (portal == nullptr) {
      out->WriteErrorResponse("ERROR:  Portal does not exist for Describe message.");
    } else if (portal->Statement()->QueryType() == network::QueryType::QUERY_SELECT) {
      out->WriteRowDescription(portal->PhysicalPlan()->GetOutputSchema()->GetColumns(), portal->ResultFormats());
    } else {
      out->WriteNoData();
    }
    return Transition::PROCEED;
  }
  TERRIER_ASSERT(object_type == DescribeCommandObjectType::STATEMENT, "Unknown object type passed in Close message.");

  const auto statement = postgres_interpreter->GetStatement(object_name);

  if (statement == nullptr) {
    out->WriteErrorResponse("ERROR:  Statement does not exist for Describe message.");
  } else {
    out->WriteParameterDescription(statement->ParamTypes());
    if (statement->QueryType() == network::QueryType::QUERY_SELECT) {
      // TODO(Matt): we're supposed to write a RowDescription here, but we don't have an OutputSchema yet because that
      // comes all the way after optimization
    } else {
      out->WriteNoData();
    }
  }

  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                const common::ManagedPointer<PostgresPacketWriter> out,
                                const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto portal_name = in_.ReadString();
  const auto max_rows UNUSED_ATTRIBUTE = in_.ReadValue<int32_t>();
  // TODO(Matt): here's where you would reason about PortalSuspend with max_rows. Maybe just the OutputWriter would
  // buffer the results, since I'm not sure if we can actually pause execution engine

  const auto portal = postgres_interpreter->GetPortal(portal_name);

  if (portal == nullptr) {
    out->WriteErrorResponse("ERROR:  Specified portal does not exist to execute.");
    return Transition::PROCEED;
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (portal->Statement()->QueryType() <= network::QueryType::QUERY_ROLLBACK) {
    t_cop->ExecuteTransactionStatement(connection, out, portal->Statement()->QueryType());
    return Transition::PROCEED;
  }

  if (portal->Statement()->QueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    out->WriteCommandComplete(portal->Statement()->QueryType(), 0);
    return Transition::PROCEED;
  }

  if (portal->PhysicalPlan() != nullptr) {
    // This happens in the event of a noop generated earlier (like in binding with an IF EXISTS);
    ExecutePortal(connection, portal, out, t_cop, false, postgres_interpreter->SingleStatementTransaction());
  } else {
    out->WriteCommandComplete(portal->Statement()->QueryType(), 0);
  }

  return Transition::PROCEED;
}

Transition SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
  if (postgres_interpreter->ImplicitTransaction()) {
    t_cop->EndTransaction(connection, connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                             : network::QueryType::QUERY_COMMIT);
    postgres_interpreter->ResetTransactionState();
  }
  out->WriteReadyForQuery(connection->TransactionState());
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();

  const auto object_type = in_.ReadValue<DescribeCommandObjectType>();
  const auto object_name = in_.ReadString();
  if (object_type == DescribeCommandObjectType::PORTAL) {
    postgres_interpreter->ClosePortal(object_name);
    out->WriteCloseComplete();
    return Transition::PROCEED;
  }
  TERRIER_ASSERT(object_type == DescribeCommandObjectType::STATEMENT, "Unknown object type passed in Close message.");
  postgres_interpreter->CloseStatement(object_name);
  out->WriteCloseComplete();
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                  const common::ManagedPointer<PostgresPacketWriter> out,
                                  const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                  const common::ManagedPointer<ConnectionContext> connection) {
  // Postgres doesn't send any sort of response for the Terminate command
  // We don't do removal of the temp namespace at the Command level because it's possible that we don't receive a
  // Terminate packet to generate the Command from, and instead closed the connection due to timeout
  return Transition::TERMINATE;
}
}  // namespace terrier::network
