#include "traffic_cop/traffic_cop.h"

#include <network/connection_context.h>

#include <future>  // NOLINT
#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "common/exception.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/execution_util.h"
#include "execution/sql/ddl_executors.h"
#include "execution/vm/module.h"
#include "network/postgres/postgres_packet_writer.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

static void CommitCallback(void *const callback_arg) {
  auto *const promise = reinterpret_cast<std::promise<bool> *const>(callback_arg);
  promise->set_value(true);
}

void TrafficCop::BeginTransaction(const common::ManagedPointer<network::ConnectionContext> connection_ctx) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE,
                 "Invalid ConnectionContext state, already in a transaction.");
  const auto txn = txn_manager_->BeginTransaction();
  connection_ctx->SetTransaction(common::ManagedPointer(txn));
  connection_ctx->SetAccessor(catalog_->GetAccessor(common::ManagedPointer(txn), connection_ctx->GetDatabaseOid()));
}

void TrafficCop::EndTransaction(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                const network::QueryType query_type) const {
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_COMMIT || query_type == network::QueryType::QUERY_ROLLBACK,
                 "EndTransaction called with invalid QueryType.");
  const auto txn = connection_ctx->Transaction();
  if (query_type == network::QueryType::QUERY_COMMIT) {
    TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                   "Invalid ConnectionContext state, not in a transaction that can be committed.");
    // Set up a blocking callback. Will be invoked when we can tell the client that commit is complete.
    std::promise<bool> promise;
    auto future = promise.get_future();
    TERRIER_ASSERT(future.valid(), "future must be valid for synchronization to work.");
    txn_manager_->Commit(txn.Get(), CommitCallback, &promise);
    future.wait();
    TERRIER_ASSERT(future.get(), "Got past the wait() without the value being set to true. That's weird.");
  } else {
    TERRIER_ASSERT(connection_ctx->TransactionState() != network::NetworkTransactionStateType::IDLE,
                   "Invalid ConnectionContext state, not in a transaction that can be aborted.");
    txn_manager_->Abort(txn.Get());
  }
  connection_ctx->SetTransaction(nullptr);
  connection_ctx->SetAccessor(nullptr);
}

void TrafficCop::HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
  TERRIER_ASSERT(replication_log_provider_ != DISABLED, "Should not be handing off logs if no log provider was given");
  replication_log_provider_->HandBufferToReplication(std::move(buffer));
}

void TrafficCop::ExecuteTransactionStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                             const common::ManagedPointer<network::PostgresPacketWriter> out,
                                             const parser::TransactionStatement::CommandType type) const {
  switch (type) {
    case parser::TransactionStatement::CommandType::kBegin: {
      TERRIER_ASSERT(connection_ctx->TransactionState() != network::NetworkTransactionStateType::FAIL,
                     "We're in an aborted state. This should have been caught already before calling this function.");
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
        out->WriteNoticeResponse("WARNING:  there is already a transaction in progress");
        out->WriteCommandComplete(network::QueryType::QUERY_BEGIN, 0);
        return;
      }
      BeginTransaction(connection_ctx);
      out->WriteCommandComplete(network::QueryType::QUERY_BEGIN, 0);
      return;
    }
    case parser::TransactionStatement::CommandType::kCommit: {
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE) {
        out->WriteNoticeResponse("WARNING:  there is no transaction in progress");
        out->WriteCommandComplete(network::QueryType::QUERY_COMMIT, 0);
        return;
      }
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::FAIL) {
        EndTransaction(connection_ctx, network::QueryType::QUERY_ROLLBACK);
        out->WriteCommandComplete(network::QueryType::QUERY_ROLLBACK, 0);
        return;
      }
      EndTransaction(connection_ctx, network::QueryType::QUERY_COMMIT);
      out->WriteCommandComplete(network::QueryType::QUERY_COMMIT, 0);
      return;
    }
    case parser::TransactionStatement::CommandType::kRollback: {
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE) {
        out->WriteNoticeResponse("WARNING:  there is no transaction in progress");
        out->WriteCommandComplete(network::QueryType::QUERY_ROLLBACK, 0);
        return;
      }
      EndTransaction(connection_ctx, network::QueryType::QUERY_ROLLBACK);
      out->WriteCommandComplete(network::QueryType::QUERY_ROLLBACK, 0);
      return;
    }
  }
}

void TrafficCop::ExecuteCreateStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                        const common::ManagedPointer<network::PostgresPacketWriter> out,
                                        const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                        const parser::CreateStatement::CreateType create_type,
                                        const bool single_statement_txn) const {
  switch (create_type) {
    case parser::CreateStatement::CreateType::kTable: {
      if (execution::sql::DDLExecutors::CreateTableExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateTablePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        out->WriteCommandComplete("CREATE TABLE");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to create table");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::CreateStatement::CreateType::kDatabase: {
      if (!single_statement_txn) {
        out->WriteErrorResponse("ERROR:  CREATE DATABASE cannot run inside a transaction block");
        connection_ctx->Transaction()->SetMustAbort();
        break;
      }
      if (execution::sql::DDLExecutors::CreateDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateDatabasePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("CREATE DATABASE");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to create database");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::CreateStatement::CreateType::kIndex: {
      if (execution::sql::DDLExecutors::CreateIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateIndexPlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("CREATE INDEX");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to create index");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::CreateStatement::CreateType::kSchema: {
      if (execution::sql::DDLExecutors::CreateNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateNamespacePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("CREATE SCHEMA");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to create schema");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported CREATE statement type");
      break;
    }
  }
}

void TrafficCop::ExecuteDropStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                      const common::ManagedPointer<network::PostgresPacketWriter> out,
                                      const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                      const parser::DropStatement::DropType drop_type,
                                      const bool single_statement_txn) const {
  switch (drop_type) {
    case parser::DropStatement::DropType::kTable: {
      if (execution::sql::DDLExecutors::DropTableExecutor(
              physical_plan.CastManagedPointerTo<planner::DropTablePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("DROP TABLE");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to drop table");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::DropStatement::DropType::kDatabase: {
      if (!single_statement_txn) {
        out->WriteErrorResponse("ERROR:  DROP DATABASE cannot run inside a transaction block");
        connection_ctx->Transaction()->SetMustAbort();
        break;
      }
      if (execution::sql::DDLExecutors::DropDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::DropDatabasePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        out->WriteCommandComplete("DROP DATABASE");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to drop database");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::DropStatement::DropType::kIndex: {
      if (execution::sql::DDLExecutors::DropIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::DropIndexPlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("DROP INDEX");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to drop index");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    case parser::DropStatement::DropType::kSchema: {
      if (execution::sql::DDLExecutors::DropNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::DropNamespacePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete("DROP SCHEMA");
        break;
      }
      out->WriteErrorResponse("ERROR:  failed to drop schema");
      connection_ctx->Transaction()->SetMustAbort();
      break;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported DROP statement type");
      break;
    }
  }
}

std::unique_ptr<parser::ParseResult> TrafficCop::ParseQuery(
    const std::string &query, const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<network::PostgresPacketWriter> out) const {
  std::unique_ptr<parser::ParseResult> parse_result;
  try {
    parse_result = parser::PostgresParser::BuildParseTree(query);
  } catch (const Exception &e) {
    // Failed to parse
    // TODO(Matt): handle this in some more verbose manner for the client (return more state)
  }
  return parse_result;

  return parse_result;
}

void TrafficCop::ExecuteSimpleQuery(const std::string &simple_query,
                                    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                    const common::ManagedPointer<network::PostgresPacketWriter> out) const {
  const auto parse_result = ParseQuery(simple_query, connection_ctx, out);

  if (parse_result == nullptr) {
    out->WriteErrorResponse("ERROR:  syntax error");
    if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
      // failing to parse fails a transaction in postgres
      connection_ctx->Transaction()->SetMustAbort();
    }
    return;
  }

  TERRIER_ASSERT(parse_result->GetStatements().size() <= 1,
                 "We currently expect one statement per string (psql and oltpbench).");

  // TODO(Matt:) some clients may send multiple statements in a single SimpleQuery packet/string. Handling that would
  // probably exist here, looping over all of the elements in the ParseResult

  // Empty queries get a special response in postgres and do not care if they're in a failed txn block
  if (parse_result->Empty()) {
    out->WriteEmptyQueryResponse();
    return;
  }

  // It parsed and we've got out single statement
  const auto statement = parse_result->GetStatement(0);
  const auto statement_type = statement->GetType();

  // Check if we're in a must-abort situation first before attempting to issue any statement other than ROLLBACK
  if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::FAIL &&
      (statement_type != parser::StatementType::TRANSACTION ||
       statement.CastManagedPointerTo<parser::TransactionStatement>()->GetTransactionType() ==
           parser::TransactionStatement::CommandType::kBegin)) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return;
  }

  ExecuteStatement(connection_ctx, out, common::ManagedPointer(parse_result), parse_result->GetStatement(0),
                   statement_type);
}

void TrafficCop::ExecuteStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                  const common::ManagedPointer<network::PostgresPacketWriter> out,
                                  const common::ManagedPointer<parser::ParseResult> parse_result,
                                  const common::ManagedPointer<parser::SQLStatement> statement,
                                  const parser::StatementType statement_type) const {
  TERRIER_ASSERT(statement->GetType() == statement_type,
                 "Called ExecuteStatement with a type that doens't match the statement.");

  switch (statement_type) {
    case parser::StatementType::TRANSACTION: {
      const auto txn_statement = statement.CastManagedPointerTo<parser::TransactionStatement>();
      ExecuteTransactionStatement(connection_ctx, out, txn_statement->GetTransactionType());
      return;
    }
    case parser::StatementType::SELECT:
    case parser::StatementType::INSERT:
    case parser::StatementType::UPDATE:
    case parser::StatementType::DELETE:
    case parser::StatementType::CREATE:
    case parser::StatementType::DROP: {
      const bool single_statement_txn =
          connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE;

      if (single_statement_txn) {
        BeginTransaction(connection_ctx);
      }

      // Next step is to try to bind the parsed statement
      // TODO(Matt): I don't think the binder should need the database name
      if (!TrafficCopUtil::Bind(connection_ctx->Accessor(), connection_ctx->GetDatabaseName(),
                                common::ManagedPointer(parse_result))) {
        out->WriteErrorResponse("ERROR:  binding failed");

        // failing to bind fails a transaction in postgres
        connection_ctx->Transaction()->SetMustAbort();

        if (single_statement_txn) {
          // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
          EndTransaction(connection_ctx, connection_ctx->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                                    : network::QueryType::QUERY_COMMIT);
        }
        return;
      }

      auto physical_plan = trafficcop::TrafficCopUtil::Optimize(
          connection_ctx->Transaction(), connection_ctx->Accessor(), common::ManagedPointer(parse_result),
          stats_storage_, optimizer_timeout_);

      if (statement_type == parser::StatementType::CREATE) {
        ExecuteCreateStatement(connection_ctx, out, common::ManagedPointer(physical_plan),
                               statement.CastManagedPointerTo<parser::CreateStatement>()->GetCreateType(),
                               single_statement_txn);
      } else if (statement_type == parser::StatementType::DROP) {
        ExecuteDropStatement(connection_ctx, out, common::ManagedPointer(physical_plan),
                             statement.CastManagedPointerTo<parser::DropStatement>()->GetDropType(),
                             single_statement_txn);
      } else {
        execution::exec::OutputPrinter printer{physical_plan->GetOutputSchema().Get()};

        auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
            connection_ctx->GetDatabaseOid(), connection_ctx->Transaction(), printer,
            physical_plan->GetOutputSchema().Get(), connection_ctx->Accessor());

        auto exec_query =
            execution::ExecutableQuery(common::ManagedPointer(physical_plan), common::ManagedPointer(exec_ctx));

        exec_query.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
      }
      if (single_statement_txn) {
        // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
        EndTransaction(connection_ctx, connection_ctx->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                                  : network::QueryType::QUERY_COMMIT);
      }

      return;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported statement type");
      return;
    }
  }
}

std::pair<catalog::db_oid_t, catalog::namespace_oid_t> TrafficCop::CreateTempNamespace(
    const network::connection_id_t connection_id, const std::string &database_name) {
  auto *const txn = txn_manager_->BeginTransaction();
  const auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name);

  if (db_oid == catalog::INVALID_DATABASE_OID) {
    // Invalid database name
    txn_manager_->Abort(txn);
    return {catalog::INVALID_DATABASE_OID, catalog::INVALID_NAMESPACE_OID};
  }

  const auto ns_oid =
      catalog_->GetAccessor(common::ManagedPointer(txn), db_oid)
          ->CreateNamespace(std::string(TEMP_NAMESPACE_PREFIX) + std::to_string(static_cast<uint16_t>(connection_id)));
  if (ns_oid == catalog::INVALID_NAMESPACE_OID) {
    // Failed to create new namespace. Could be a concurrent DDL change and worth retrying
    txn_manager_->Abort(txn);
    return {db_oid, catalog::INVALID_NAMESPACE_OID};
  }

  // Success
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return {db_oid, ns_oid};
}

bool TrafficCop::DropTempNamespace(const catalog::db_oid_t db_oid, const catalog::namespace_oid_t ns_oid) {
  TERRIER_ASSERT(db_oid != catalog::INVALID_DATABASE_OID, "Called DropTempNamespace() with an invalid database oid.");
  TERRIER_ASSERT(ns_oid != catalog::INVALID_NAMESPACE_OID, "Called DropTempNamespace() with an invalid namespace oid.");
  auto *const txn = txn_manager_->BeginTransaction();
  const auto db_accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);

  TERRIER_ASSERT(db_accessor != nullptr, "Catalog failed to provide a CatalogAccessor. Was the db_oid still valid?");

  const auto result = db_accessor->DropNamespace(ns_oid);
  if (result) {
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  } else {
    txn_manager_->Abort(txn);
  }
  return result;
}

}  // namespace terrier::trafficcop
