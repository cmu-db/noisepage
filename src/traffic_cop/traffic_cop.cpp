#include "traffic_cop/traffic_cop.h"

#include <future>  // NOLINT
#include <memory>
#include <string>
#include <utility>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "common/exception.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/executable_query.h"
#include "execution/sql/ddl_executors.h"
#include "execution/vm/module.h"
#include "network/connection_context.h"
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
                                             const terrier::network::QueryType query_type) const {
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_COMMIT || query_type == network::QueryType::QUERY_ROLLBACK ||
                     query_type == network::QueryType::QUERY_BEGIN,
                 "ExecuteTransactionStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_BEGIN: {
      TERRIER_ASSERT(connection_ctx->TransactionState() != network::NetworkTransactionStateType::FAIL,
                     "We're in an aborted state. This should have been caught already before calling this function.");
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
        out->WriteNoticeResponse("WARNING:  there is already a transaction in progress");
        break;
      }
      BeginTransaction(connection_ctx);
      break;
    }
    case network::QueryType::QUERY_COMMIT: {
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE) {
        out->WriteNoticeResponse("WARNING:  there is no transaction in progress");
        break;
      }
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::FAIL) {
        EndTransaction(connection_ctx, network::QueryType::QUERY_ROLLBACK);
        out->WriteCommandComplete(network::QueryType::QUERY_ROLLBACK, 0);
        return;
      }
      EndTransaction(connection_ctx, network::QueryType::QUERY_COMMIT);
      break;
    }
    case network::QueryType::QUERY_ROLLBACK: {
      if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::IDLE) {
        out->WriteNoticeResponse("WARNING:  there is no transaction in progress");
        break;
      }
      EndTransaction(connection_ctx, network::QueryType::QUERY_ROLLBACK);
      break;
    }
    default:
      UNREACHABLE("ExecuteTransactionStatement called with invalid QueryType.");
  }
  out->WriteCommandComplete(query_type, 0);
}

void TrafficCop::ExecuteCreateStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                        const common::ManagedPointer<network::PostgresPacketWriter> out,
                                        const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                        const terrier::network::QueryType query_type,
                                        const bool single_statement_txn) const {
  TERRIER_ASSERT(
      query_type == network::QueryType::QUERY_CREATE_TABLE || query_type == network::QueryType::QUERY_CREATE_SCHEMA ||
          query_type == network::QueryType::QUERY_CREATE_INDEX || query_type == network::QueryType::QUERY_CREATE_DB ||
          query_type == network::QueryType::QUERY_CREATE_VIEW || query_type == network::QueryType::QUERY_CREATE_TRIGGER,
      "ExecuteCreateStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_CREATE_TABLE: {
      if (execution::sql::DDLExecutors::CreateTableExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateTablePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_DB: {
      if (!single_statement_txn) {
        out->WriteErrorResponse("ERROR:  CREATE DATABASE cannot run inside a transaction block");
        connection_ctx->Transaction()->SetMustAbort();
        return;
      }
      if (execution::sql::DDLExecutors::CreateDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateDatabasePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_INDEX: {
      if (execution::sql::DDLExecutors::CreateIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateIndexPlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_SCHEMA: {
      if (execution::sql::DDLExecutors::CreateNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateNamespacePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported CREATE statement type");
      return;
    }
  }
  out->WriteErrorResponse("ERROR:  failed to execute CREATE");
  connection_ctx->Transaction()->SetMustAbort();
}

void TrafficCop::ExecuteDropStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                      const common::ManagedPointer<network::PostgresPacketWriter> out,
                                      const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                      const terrier::network::QueryType query_type,
                                      const bool single_statement_txn) const {
  TERRIER_ASSERT(
      query_type == network::QueryType::QUERY_DROP_TABLE || query_type == network::QueryType::QUERY_DROP_SCHEMA ||
          query_type == network::QueryType::QUERY_DROP_INDEX || query_type == network::QueryType::QUERY_DROP_DB ||
          query_type == network::QueryType::QUERY_DROP_VIEW || query_type == network::QueryType::QUERY_DROP_TRIGGER,
      "ExecuteDropStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_DROP_TABLE: {
      if (execution::sql::DDLExecutors::DropTableExecutor(
              physical_plan.CastManagedPointerTo<planner::DropTablePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_DROP_DB: {
      if (!single_statement_txn) {
        out->WriteErrorResponse("ERROR:  DROP DATABASE cannot run inside a transaction block");
        connection_ctx->Transaction()->SetMustAbort();
        return;
      }
      if (execution::sql::DDLExecutors::DropDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::DropDatabasePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_DROP_INDEX: {
      if (execution::sql::DDLExecutors::DropIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::DropIndexPlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    case network::QueryType::QUERY_DROP_SCHEMA: {
      if (execution::sql::DDLExecutors::DropNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::DropNamespacePlanNode>(), connection_ctx->Accessor())) {
        out->WriteCommandComplete(query_type, 0);
        return;
      }
      break;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported DROP statement type");
      break;
    }
  }
  out->WriteErrorResponse("ERROR:  failed to execute DROP");
  connection_ctx->Transaction()->SetMustAbort();
}

std::unique_ptr<parser::ParseResult> TrafficCop::ParseQuery(
    const std::string &query, const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<network::PostgresPacketWriter> out) const {
  std::unique_ptr<parser::ParseResult> parse_result;
  try {
    parse_result = parser::PostgresParser::BuildParseTree(query);
  } catch (...) {
    // Failed to parse
    // TODO(Matt): handle this in some more verbose manner for the client (return more state)
  }
  return parse_result;
}

bool TrafficCop::BindStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                               const common::ManagedPointer<network::PostgresPacketWriter> out,
                               const common::ManagedPointer<parser::ParseResult> parse_result,
                               const terrier::network::QueryType query_type) const {
  try {
    // TODO(Matt): I don't think the binder should need the database name. It's already bound in the ConnectionContext
    binder::BindNodeVisitor visitor(connection_ctx->Accessor(), connection_ctx->GetDatabaseName());
    visitor.BindNameToNode(parse_result->GetStatement(0), parse_result.Get());
  } catch (...) {
    // Failed to bind
    // TODO(Matt): this is a hack to get IF EXISTS to work with our tests, we actually need better support in
    // PostgresParser and the binder should return more state back to the TrafficCop to figure out what to do
    if ((parse_result->GetStatement(0)->GetType() == parser::StatementType::DROP &&
         parse_result->GetStatement(0).CastManagedPointerTo<parser::DropStatement>()->IsIfExists())) {
      out->WriteNoticeResponse("NOTICE:  binding failed with an IF EXISTS clause, skipping statement");
      out->WriteCommandComplete(query_type, 0);
    } else {
      out->WriteErrorResponse("ERROR:  binding failed");
      // failing to bind fails a transaction in postgres
      connection_ctx->Transaction()->SetMustAbort();
    }
    return false;
  }
  return true;
}

void TrafficCop::ExecuteStatement(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                  const common::ManagedPointer<network::PostgresPacketWriter> out,
                                  const common::ManagedPointer<parser::ParseResult> parse_result,
                                  const terrier::network::QueryType query_type) const {
  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (query_type <= network::QueryType::QUERY_ROLLBACK) {
    ExecuteTransactionStatement(connection_ctx, out, query_type);
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
    BeginTransaction(connection_ctx);
  }

  // Try to bind the parsed statement
  if (BindStatement(connection_ctx, out, parse_result, query_type)) {
    // Binding succeeded, optimize to generate a physical plan and then execute
    auto physical_plan = trafficcop::TrafficCopUtil::Optimize(connection_ctx->Transaction(), connection_ctx->Accessor(),
                                                              parse_result, stats_storage_, optimizer_timeout_);

    // This logic relies on ordering of values in the enum's definition and is documented there as well.
    if (query_type <= network::QueryType::QUERY_DELETE) {
      // DML query to put through codegen
      CodegenAndRunPhysicalPlan(connection_ctx, out, common::ManagedPointer(physical_plan), query_type);
    } else if (query_type <= network::QueryType::QUERY_CREATE_VIEW) {
      ExecuteCreateStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                             single_statement_txn);
    } else if (query_type <= network::QueryType::QUERY_DROP_VIEW) {
      ExecuteDropStatement(connection_ctx, out, common::ManagedPointer(physical_plan), query_type,
                           single_statement_txn);
    }
  }

  if (single_statement_txn) {
    // Single statement transaction should be ended before returning
    // decide whether the txn should be committed or aborted based on the MustAbort flag, and then end the txn
    EndTransaction(connection_ctx, connection_ctx->Transaction()->MustAbort() ? network::QueryType::QUERY_ROLLBACK
                                                                              : network::QueryType::QUERY_COMMIT);
  }
}

void TrafficCop::CodegenAndRunPhysicalPlan(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                           const common::ManagedPointer<network::PostgresPacketWriter> out,
                                           const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
                                           const terrier::network::QueryType query_type) const {
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_SELECT || query_type == network::QueryType::QUERY_INSERT ||
                     query_type == network::QueryType::QUERY_UPDATE || query_type == network::QueryType::QUERY_DELETE,
                 "CodegenAndRunPhysicalPlan called with invalid QueryType.");
  execution::exec::OutputWriter writer(physical_plan->GetOutputSchema(), out);

  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      connection_ctx->GetDatabaseOid(), connection_ctx->Transaction(), writer, physical_plan->GetOutputSchema().Get(),
      connection_ctx->Accessor());

  auto exec_query = execution::ExecutableQuery(common::ManagedPointer(physical_plan), common::ManagedPointer(exec_ctx));

  if (query_type == network::QueryType::QUERY_SELECT)
    out->WriteRowDescription(physical_plan->GetOutputSchema()->GetColumns());

  exec_query.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);

  if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
    // Execution didn't set us to FAIL state, go ahead and write command complete

    if (query_type == network::QueryType::QUERY_SELECT) {
      // For selects we really on the OutputWriter to store the number of rows affected because sequential scan
      // iteration can happen in multiple pipelines
      out->WriteCommandComplete(query_type, writer.NumRows());

    } else {
      // Other queries (INSERT, UPDATE, DELETE) retrieve rows affected from the execution context since other queries
      // might not have any output otherwise
      out->WriteCommandComplete(query_type, exec_ctx->RowsAffected());
    }

  } else {
    // TODO(Matt): We need a more verbose way to say what happened during execution (INSERT failed for key conflict,
    // etc.) I suspect we would stash that in the ExecutionContext.
    out->WriteErrorResponse("Query failed.");
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
