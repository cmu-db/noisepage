#include "network/postgres/postgres_network_commands.h"

#include <memory>
#include <string>
#include <variant>

#include "network/network_util.h"
#include "network/postgres/postgres_packet_util.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/postgres/statement.h"
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
                          const common::ManagedPointer<trafficcop::TrafficCop> t_cop, const bool explicit_txn_block) {
  trafficcop::TrafficCopResult result;

  const auto query_type = portal->GetStatement()->GetQueryType();
  const auto physical_plan = portal->PhysicalPlan();

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::DMLQueryType(query_type)) {
    // DML query to put through codegen
    result = t_cop->CodegenPhysicalPlan(connection_ctx, out, portal);

    // TODO(Matt): do something with result here in case codegen fails

    result = t_cop->RunExecutableQuery(connection_ctx, out, portal);
  } else if (NetworkUtil::CreateQueryType(query_type)) {
    if (explicit_txn_block && query_type == network::QueryType::QUERY_CREATE_DB) {
      out->WriteErrorResponse("ERROR:  CREATE DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    if (explicit_txn_block && query_type == network::QueryType::QUERY_CREATE_INDEX &&
        physical_plan.CastManagedPointerTo<planner::CreateIndexPlanNode>()->GetConcurrent()) {
      out->WriteErrorResponse("ERROR:  CREATE INDEX CONCURRENTLY cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    if (query_type == network::QueryType::QUERY_CREATE_INDEX) {
      auto create_index_plan = physical_plan.CastManagedPointerTo<planner::CreateIndexPlanNode>();
      if (!create_index_plan->GetConcurrent()) {
        auto table_oid = create_index_plan->GetTableOid();
        if (connection_ctx->Transaction()->IsTableLocked(table_oid)) {
          out->WriteErrorResponse(
              "ERROR:  CREATE INDEX cannot be called with uncommitted modifications to table in same transaction");
          return;
        }
        auto table_lock = connection_ctx->Accessor()->GetTableLock(table_oid);
        table_lock->lock();
        result = t_cop->CodegenPhysicalPlan(connection_ctx, out, portal);

        //TODO(Wuwen): grab lock for creation
        result = t_cop->RunExecutableQuery(connection_ctx, out, portal);
        table_lock->unlock();
        result = {trafficcop::ResultType::COMPLETE, 0};

      } else {
        //concurrent
        result = t_cop->CodegenPhysicalPlan(connection_ctx, out, portal);

        result = t_cop->RunExecutableQuery(connection_ctx, out, portal);
      }

    } else {
      result = t_cop->ExecuteCreateStatement(connection_ctx, physical_plan, query_type);
    }

  } else if (NetworkUtil::DropQueryType(query_type)) {
    if (explicit_txn_block && query_type == network::QueryType::QUERY_DROP_DB) {
      out->WriteErrorResponse("ERROR:  DROP DATABASE cannot run inside a transaction block");
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    result = t_cop->ExecuteDropStatement(connection_ctx, physical_plan, query_type);
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
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

  auto query_text = in_.ReadString();

  auto parse_result = t_cop->ParseQuery(query_text, connection);

  const auto statement = std::make_unique<network::Statement>(std::move(query_text), std::move(parse_result));

  // Parsing a SimpleQuery clears the unnamed statement and portal
  postgres_interpreter->CloseStatement("");
  postgres_interpreter->ClosePortal("");

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

  const auto query_type = statement->GetQueryType();

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      query_type != QueryType::QUERY_COMMIT && query_type != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return FinishSimpleQueryCommand(out, connection);
  }

  // Begin a transaction, regardless of statement type. If it's a BEGIN statement it's implicitly in this txn
  if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
    TERRIER_ASSERT(!postgres_interpreter->ExplicitTransactionBlock(),
                   "We shouldn't be in an explicit txn block is transaction state is IDLE.");
    t_cop->BeginTransaction(connection);
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::TransactionalQueryType(query_type)) {
    t_cop->ExecuteTransactionStatement(connection, out, postgres_interpreter->ExplicitTransactionBlock(), query_type);
    if (query_type == network::QueryType::QUERY_BEGIN) {
      postgres_interpreter->SetExplicitTransactionBlock();
    } else {
      postgres_interpreter->ResetTransactionState();
    }
    return FinishSimpleQueryCommand(out, connection);
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::UnsupportedQueryType(query_type)) {
    // We don't yet support query types with values greater than this
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
    out->WriteCommandComplete(query_type, 0);
  } else {
    // Try to bind the parsed statement
    const auto bind_result = t_cop->BindQuery(connection, common::ManagedPointer(statement), nullptr);
    if (bind_result.type_ == trafficcop::ResultType::COMPLETE) {
      // Binding succeeded, optimize to generate a physical plan and then execute
      auto physical_plan = t_cop->OptimizeBoundQuery(connection, statement->ParseResult());

      statement->SetPhysicalPlan(std::move(physical_plan));

      const auto portal = std::make_unique<Portal>(common::ManagedPointer(statement));

      if (query_type == network::QueryType::QUERY_SELECT) {
        out->WriteRowDescription(portal->PhysicalPlan()->GetOutputSchema()->GetColumns(), portal->ResultFormats());
      }

      ExecutePortal(connection, common::ManagedPointer(portal), out, t_cop,
                    postgres_interpreter->ExplicitTransactionBlock());
    } else if (bind_result.type_ == trafficcop::ResultType::NOTICE) {
      TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
      out->WriteNoticeResponse(std::get<std::string>(bind_result.extra_));
      out->WriteCommandComplete(query_type, 0);
    } else {
      TERRIER_ASSERT(bind_result.type_ == trafficcop::ResultType::ERROR,
                     "I don't think we expect any other ResultType at this point.");
      TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
      // failing to bind fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
      out->WriteErrorResponse(std::get<std::string>(bind_result.extra_));
    }
  }

  if (!postgres_interpreter->ExplicitTransactionBlock()) {
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
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

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

  auto query_text = in_.ReadString();
  auto parse_result = t_cop->ParseQuery(query_text, connection);
  auto param_types = PostgresPacketUtil::ReadParamTypes(common::ManagedPointer(&in_));

  auto statement =
      std::make_unique<network::Statement>(std::move(query_text), std::move(parse_result), std::move(param_types));

  // Extended Query protocol doesn't allow for more than one statement per query string
  if (!statement->Valid() || statement->ParseResult()->NumStatements() > 1) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection->Transaction()->SetMustAbort();
    }
    postgres_interpreter->SetWaitingForSync();
    return Transition::PROCEED;
  }

  if (statement->GetQueryType() >= network::QueryType::QUERY_RENAME) {
    // We don't yet support query types with values greater than this
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
  }

  auto cached_statement = postgres_interpreter->LookupStatementInCache(statement->GetQueryText());
  if (cached_statement == nullptr) {
    // Not in the cache, add to cache
    cached_statement = common::ManagedPointer(statement);
    postgres_interpreter->AddStatementToCache(fingerprint_result.hexdigest, std::move(statement));
  }

  postgres_interpreter->SetStatement(statement_name, cached_statement);

  out->WriteParseComplete();
  return Transition::PROCEED;
}

Transition BindCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                             const common::ManagedPointer<PostgresPacketWriter> out,
                             const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

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

  const auto query_type = statement->GetQueryType();

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
  // TODO(Matt): probably shouldn't be an assert, but rather an error response

  // read the params
  auto params =
      PostgresPacketUtil::ReadParameters(common::ManagedPointer(&in_), statement->ParamTypes(), param_formats);

  // read out the result formats
  auto result_formats = PostgresPacketUtil::ReadFormatCodes(common::ManagedPointer(&in_));
  // TODO(Matt): would like to assert here that this is 0 (all text), 1 (all the same), or the number of output columns
  // but we can't do that without an OutputSchema yet this early in the pipeline

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      query_type != QueryType::QUERY_COMMIT && query_type != QueryType::QUERY_ROLLBACK) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return Transition::PROCEED;
  }

  // Begin a transaction, regardless of statement type. If it's a BEGIN statement it's implicitly in this txn
  if (connection->TransactionState() == network::NetworkTransactionStateType::IDLE) {
    TERRIER_ASSERT(!postgres_interpreter->ExplicitTransactionBlock(),
                   "We shouldn't be in an explicit txn block is transaction state is IDLE.");
    t_cop->BeginTransaction(connection);
  }

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::TransactionalQueryType(query_type)) {
    // Don't begin an implicit txn in this case, and don't bind or optimize this statement
    postgres_interpreter->SetPortal(portal_name,
                                    std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
    out->WriteBindComplete();
    return Transition::PROCEED;
  }

  if (NetworkUtil::UnsupportedQueryType(query_type)) {
    // We don't yet support query types with values greater than this
    // Don't begin an implicit txn in this case, and don't bind or optimize this statement. Just noop with a Notice (not
    // an Error) and proceed to reading more messages.
    postgres_interpreter->SetPortal(portal_name,
                                    std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
    out->WriteNoticeResponse("NOTICE:  we don't yet support that query type.");
    out->WriteBindComplete();
    return Transition::PROCEED;
  }

  if (UNLIKELY(NetworkUtil::DDLQueryType(query_type))) {
    statement->ClearCachedObjects();
  }

  // Bind it, plan it
  const auto bind_result = t_cop->BindQuery(connection, statement, common::ManagedPointer(&params));
  if (LIKELY(bind_result.type_ == trafficcop::ResultType::COMPLETE)) {
    // Binding succeeded, optimize to generate a physical plan
    if (statement->PhysicalPlan() == nullptr || !t_cop->UseQueryCache()) {
      // it's not cached, optimize it
      auto physical_plan = t_cop->OptimizeBoundQuery(connection, statement->ParseResult());
      statement->SetPhysicalPlan(std::move(physical_plan));
    }

    postgres_interpreter->SetPortal(portal_name,
                                    std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
    out->WriteBindComplete();
  } else if (UNLIKELY(bind_result.type_ == trafficcop::ResultType::NOTICE)) {
    // Binding generated a NOTICE, i.e. IF EXISTS failed, so we're not going to generate a physical plan of nullptr and
    // handle that case in Execute. In case it previously bound and compiled, we're gonna throw that away for next
    // execution
    statement->ClearCachedObjects();
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    postgres_interpreter->SetPortal(portal_name,
                                    std::make_unique<Portal>(statement, std::move(params), std::move(result_formats)));
    out->WriteNoticeResponse(std::get<std::string>(bind_result.extra_));
    out->WriteBindComplete();
  } else {
    TERRIER_ASSERT(bind_result.type_ == trafficcop::ResultType::ERROR,
                   "I don't think we expect any other ResultType at this point.");
    TERRIER_ASSERT(std::holds_alternative<std::string>(bind_result.extra_), "We're expecting a message here.");
    // failing to bind fails a transaction in postgres
    connection->Transaction()->SetMustAbort();
    // clear anything cached related to this statement
    statement->ClearCachedObjects();
    out->WriteErrorResponse(std::get<std::string>(bind_result.extra_));
    postgres_interpreter->SetWaitingForSync();
  }

  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                                 const common::ManagedPointer<PostgresPacketWriter> out,
                                 const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                 const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

  const auto object_type = in_.ReadValue<DescribeCommandObjectType>();
  const auto object_name = in_.ReadString();

  if (object_type == DescribeCommandObjectType::PORTAL) {
    const auto portal = postgres_interpreter->GetPortal(object_name);
    if (portal == nullptr) {
      out->WriteErrorResponse("ERROR:  Portal does not exist for Describe message.");
    } else if (portal->GetStatement()->GetQueryType() == network::QueryType::QUERY_SELECT) {
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
    if (statement->GetQueryType() == network::QueryType::QUERY_SELECT) {
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
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

  const auto portal_name = in_.ReadString();
  const auto max_rows UNUSED_ATTRIBUTE = in_.ReadValue<int32_t>();
  // TODO(Matt): here's where you would reason about PortalSuspend with max_rows. Maybe just the OutputWriter would
  // buffer the results, since I'm not sure if we can actually pause execution engine

  const auto portal = postgres_interpreter->GetPortal(portal_name);

  if (portal == nullptr) {
    out->WriteErrorResponse("ERROR:  Specified portal does not exist to execute.");
    return Transition::PROCEED;
  }

  const auto statement = portal->GetStatement();
  const auto query_type = statement->GetQueryType();

  // TODO(Matt): Probably handle EmptyStatement around here somewhere, maybe somewhere more general purpose though?

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::TransactionalQueryType(query_type)) {
    t_cop->ExecuteTransactionStatement(connection, out, postgres_interpreter->ExplicitTransactionBlock(), query_type);
    if (query_type == network::QueryType::QUERY_BEGIN) {
      postgres_interpreter->SetExplicitTransactionBlock();
    } else {
      postgres_interpreter->ResetTransactionState();
    }
    return Transition::PROCEED;
  }

  if (NetworkUtil::UnsupportedQueryType(query_type)) {
    // We don't yet support query types with values greater than this
    out->WriteCommandComplete(query_type, 0);
    return Transition::PROCEED;
  }

  if (portal->PhysicalPlan() != nullptr) {
    ExecutePortal(connection, portal, out, t_cop, postgres_interpreter->ExplicitTransactionBlock());
    if (connection->TransactionState() == NetworkTransactionStateType::FAIL) {
      postgres_interpreter->SetWaitingForSync();
    }
  } else {
    // This happens in the event of a noop generated earlier (like in binding with an IF EXISTS);
    out->WriteCommandComplete(query_type, 0);
  }

  return Transition::PROCEED;
}

Transition SyncCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                             common::ManagedPointer<PostgresPacketWriter> out,
                             common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
  if (!postgres_interpreter->ExplicitTransactionBlock() &&
      !(connection->TransactionState() == network::NetworkTransactionStateType::IDLE)) {
    t_cop->EndTransaction(connection, connection->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                             : network::QueryType::QUERY_COMMIT);
    postgres_interpreter->ResetTransactionState();
  } else if (postgres_interpreter->WaitingForSync()) {
    postgres_interpreter->ResetWaitingForSync();
  }
  out->WriteReadyForQuery(connection->TransactionState());
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(const common::ManagedPointer<ProtocolInterpreter> interpreter,
                              const common::ManagedPointer<PostgresPacketWriter> out,
                              const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                              const common::ManagedPointer<ConnectionContext> connection) {
  const auto postgres_interpreter = interpreter.CastManagedPointerTo<network::PostgresProtocolInterpreter>();
  TERRIER_ASSERT(!postgres_interpreter->WaitingForSync(),
                 "We shouldn't be trying to execute commands while waiting for Sync message. This should have been "
                 "caught at the protocol interpreter Process() level.");

  const auto object_type = in_.ReadValue<DescribeCommandObjectType>();
  const auto object_name = in_.ReadString();
  if (object_type == DescribeCommandObjectType::PORTAL) {
    postgres_interpreter->ClosePortal(object_name);
    // "It is not an error to issue Close against a nonexistent statement or portal name."
    out->WriteCloseComplete();
    return Transition::PROCEED;
  }
  TERRIER_ASSERT(object_type == DescribeCommandObjectType::STATEMENT, "Unknown object type passed in Close message.");
  postgres_interpreter->CloseStatement(object_name);
  // "It is not an error to issue Close against a nonexistent statement or portal name."
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

// (Matt): this seems to only exist for testing
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
