#include "traffic_cop/traffic_cop.h"

#include <future>  // NOLINT
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/postgres/builder.h"
#include "common/exception.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/executable_query.h"
#include "execution/sql/ddl_executors.h"
#include "execution/vm/module.h"
#include "network/connection_context.h"
#include "network/postgres/portal.h"
#include "network/postgres/postgres_packet_writer.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/postgres/statement.h"
#include "optimizer/abstract_optimizer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/query_to_operator_transformer.h"
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
                                             const bool explicit_txn_block,
                                             const terrier::network::QueryType query_type) const {
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_COMMIT || query_type == network::QueryType::QUERY_ROLLBACK ||
                     query_type == network::QueryType::QUERY_BEGIN,
                 "ExecuteTransactionStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_BEGIN: {
      TERRIER_ASSERT(connection_ctx->TransactionState() != network::NetworkTransactionStateType::FAIL,
                     "We're in an aborted state. This should have been caught already before calling this function.");
      if (explicit_txn_block) {
        out->WriteNoticeResponse("WARNING:  there is already a transaction in progress");
        break;
      }
      break;
    }
    case network::QueryType::QUERY_COMMIT: {
      if (!explicit_txn_block) {
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
      if (!explicit_txn_block) {
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

std::unique_ptr<planner::AbstractPlanNode> TrafficCop::OptimizeBoundQuery(
    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<parser::ParseResult> query) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");

  return TrafficCopUtil::Optimize(connection_ctx->Transaction(), connection_ctx->Accessor(), query,
                                  connection_ctx->GetDatabaseOid(), stats_storage_,
                                  std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout_);
}

TrafficCopResult TrafficCop::ExecuteCreateStatement(
    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
    const terrier::network::QueryType query_type) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");
  TERRIER_ASSERT(
      query_type == network::QueryType::QUERY_CREATE_TABLE || query_type == network::QueryType::QUERY_CREATE_SCHEMA ||
          query_type == network::QueryType::QUERY_CREATE_INDEX || query_type == network::QueryType::QUERY_CREATE_DB ||
          query_type == network::QueryType::QUERY_CREATE_VIEW || query_type == network::QueryType::QUERY_CREATE_TRIGGER ||
          query_type == network::QueryType::QUERY_CREATE_SEQUENCE,
      "ExecuteCreateStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_CREATE_TABLE: {
      if (execution::sql::DDLExecutors::CreateTableExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateTablePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_DB: {
      if (execution::sql::DDLExecutors::CreateDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateDatabasePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_INDEX: {
      if (execution::sql::DDLExecutors::CreateIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateIndexPlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_SCHEMA: {
      if (execution::sql::DDLExecutors::CreateNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateNamespacePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_CREATE_SEQUENCE: {
      if (execution::sql::DDLExecutors::CreateSequenceExecutor(
              physical_plan.CastManagedPointerTo<planner::CreateSequencePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    default: {
      return {ResultType::ERROR, "ERROR:  unsupported CREATE statement type"};
    }
  }
  connection_ctx->Transaction()->SetMustAbort();
  return {ResultType::ERROR, "ERROR:  failed to execute CREATE"};
}

TrafficCopResult TrafficCop::ExecuteDropStatement(
    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<planner::AbstractPlanNode> physical_plan,
    const terrier::network::QueryType query_type) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");
  TERRIER_ASSERT(
      query_type == network::QueryType::QUERY_DROP_TABLE || query_type == network::QueryType::QUERY_DROP_SCHEMA ||
          query_type == network::QueryType::QUERY_DROP_INDEX || query_type == network::QueryType::QUERY_DROP_DB ||
          query_type == network::QueryType::QUERY_DROP_VIEW || query_type == network::QueryType::QUERY_DROP_TRIGGER ||
          query_type == network::QueryType::QUERY_DROP_SEQUENCE,
      "ExecuteDropStatement called with invalid QueryType.");
  switch (query_type) {
    case network::QueryType::QUERY_DROP_TABLE: {
      if (execution::sql::DDLExecutors::DropTableExecutor(
              physical_plan.CastManagedPointerTo<planner::DropTablePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_DROP_DB: {
      if (execution::sql::DDLExecutors::DropDatabaseExecutor(
              physical_plan.CastManagedPointerTo<planner::DropDatabasePlanNode>(), connection_ctx->Accessor(),
              connection_ctx->GetDatabaseOid())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_DROP_INDEX: {
      if (execution::sql::DDLExecutors::DropIndexExecutor(
              physical_plan.CastManagedPointerTo<planner::DropIndexPlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_DROP_SCHEMA: {
      if (execution::sql::DDLExecutors::DropNamespaceExecutor(
              physical_plan.CastManagedPointerTo<planner::DropNamespacePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    case network::QueryType::QUERY_DROP_SEQUENCE: {
      if (execution::sql::DDLExecutors::DropSequenceExecutor(
              physical_plan.CastManagedPointerTo<planner::DropSequencePlanNode>(), connection_ctx->Accessor())) {
        return {ResultType::COMPLETE, 0};
      }
      break;
    }
    default: {
      return {ResultType::ERROR, "ERROR:  unsupported DROP statement type"};
    }
  }
  connection_ctx->Transaction()->SetMustAbort();
  return {ResultType::ERROR, "ERROR:  failed to execute DROP"};
}

std::unique_ptr<parser::ParseResult> TrafficCop::ParseQuery(
    const std::string &query, const common::ManagedPointer<network::ConnectionContext> connection_ctx) const {
  std::unique_ptr<parser::ParseResult> parse_result;
  try {
    parse_result = parser::PostgresParser::BuildParseTree(query);
  } catch (...) {
    // Failed to parse
    // TODO(Matt): handle this in some more verbose manner for the client (return more state)
  }
  return parse_result;
}

TrafficCopResult TrafficCop::BindQuery(
    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<network::Statement> statement,
    const common::ManagedPointer<std::vector<type::TransientValue>> parameters) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");
  try {
    binder::BindNodeVisitor visitor(connection_ctx->Accessor(), connection_ctx->GetDatabaseOid());
    visitor.BindNameToNode(statement->ParseResult(), parameters);
  } catch (...) {
    // Failed to bind
    // TODO(Matt): this is a hack to get IF EXISTS to work with our tests, we actually need better support in
    // PostgresParser and the binder should return more state back to the TrafficCop to figure out what to do
    if ((statement->RootStatement()->GetType() == parser::StatementType::DROP &&
         statement->RootStatement().CastManagedPointerTo<parser::DropStatement>()->IsIfExists())) {
      return {ResultType::NOTICE, "NOTICE:  binding failed with an IF EXISTS clause, skipping statement"};
    }
    return {ResultType::ERROR, "ERROR:  binding failed"};
  }
  return {ResultType::COMPLETE, 0};
}

TrafficCopResult TrafficCop::CodegenPhysicalPlan(
    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
    const common::ManagedPointer<network::PostgresPacketWriter> out,
    const common::ManagedPointer<network::Portal> portal) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");
  const auto query_type UNUSED_ATTRIBUTE = portal->GetStatement()->GetQueryType();
  const auto physical_plan = portal->PhysicalPlan();
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_SELECT || query_type == network::QueryType::QUERY_INSERT ||
                     query_type == network::QueryType::QUERY_UPDATE || query_type == network::QueryType::QUERY_DELETE,
                 "CodegenAndRunPhysicalPlan called with invalid QueryType.");

  if (portal->GetStatement()->GetExecutableQuery() != nullptr && use_query_cache_) {
    // We've already codegen'd this, move on...
    return {ResultType::COMPLETE, 0};
  }
  // TODO(Matt): We should get rid of the need of an OutputWriter to ExecutionContext since we just throw this one away
  execution::exec::OutputWriter writer(physical_plan->GetOutputSchema(), out, portal->ResultFormats());
  // TODO(Matt): We should get rid of the need of an ExecutionContext to perform codegen since we just throw this one
  // away
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      connection_ctx->GetDatabaseOid(), connection_ctx->Transaction(), writer, physical_plan->GetOutputSchema().Get(),
      connection_ctx->Accessor());
  auto exec_query = std::make_unique<execution::ExecutableQuery>(common::ManagedPointer(physical_plan),
                                                                 common::ManagedPointer(exec_ctx));
  // TODO(Matt): handle code generation failing
  portal->GetStatement()->SetExecutableQuery(std::move(exec_query));
  return {ResultType::COMPLETE, 0};
}

TrafficCopResult TrafficCop::RunExecutableQuery(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                                const common::ManagedPointer<network::PostgresPacketWriter> out,
                                                const common::ManagedPointer<network::Portal> portal) const {
  TERRIER_ASSERT(connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK,
                 "Not in a valid txn. This should have been caught before calling this function.");
  const auto query_type = portal->GetStatement()->GetQueryType();
  const auto physical_plan = portal->PhysicalPlan();
  TERRIER_ASSERT(query_type == network::QueryType::QUERY_SELECT || query_type == network::QueryType::QUERY_INSERT ||
                     query_type == network::QueryType::QUERY_UPDATE || query_type == network::QueryType::QUERY_DELETE,
                 "CodegenAndRunPhysicalPlan called with invalid QueryType.");
  execution::exec::OutputWriter writer(physical_plan->GetOutputSchema(), out, portal->ResultFormats());

  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      connection_ctx->GetDatabaseOid(), connection_ctx->Transaction(), writer, physical_plan->GetOutputSchema().Get(),
      connection_ctx->Accessor(), connection_ctx->GetTempNamespaceOid(), connection_ctx->GetTempTable());

  exec_ctx->SetParams(portal->Parameters());

  const auto exec_query = portal->GetStatement()->GetExecutableQuery();

  // builtin function currval will throw exception.
  try {
    exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  }catch (Exception& exception) {
      return {ResultType::ERROR, "Query failed. " + std::string(exception.what())};
  }


  if (connection_ctx->TransactionState() == network::NetworkTransactionStateType::BLOCK) {
    // Execution didn't set us to FAIL state, go ahead and return command complete
    if (query_type == network::QueryType::QUERY_SELECT) {
      // For selects we rely on the OutputWriter to store the number of rows affected because sequential scan
      // iteration can happen in multiple pipelines
      return {ResultType::COMPLETE, writer.NumRows()};
    }
    // Other queries (INSERT, UPDATE, DELETE) retrieve rows affected from the execution context since other queries
    // might not have any output otherwise
    return {ResultType::COMPLETE, exec_ctx->RowsAffected()};
  }

  // TODO(Matt): We need a more verbose way to say what happened during execution (INSERT failed for key conflict,
  // etc.) I suspect we would stash that in the ExecutionContext.
  return {ResultType::ERROR, "Query failed."};
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

catalog::table_oid_t TrafficCop::CreateTempTable(
    const catalog::db_oid_t db_oid,  const catalog::namespace_oid_t ns_oid, const network::connection_id_t connection_id) {
  auto *const txn = txn_manager_->BeginTransaction();

  const catalog::Schema temp_schema = catalog::postgres::Builder::GetSequenceTempTableSchema();

//  const auto temp_table_oid =
//      catalog_->GetAccessor(common::ManagedPointer(txn), db_oid)->CreateTable((catalog::namespace_oid_t)15,
//          "temp_table_" + std::to_string((uint32_t)ns_oid), temp_schema);

    const auto temp_table_oid =
            catalog_->GetAccessor(common::ManagedPointer(txn), db_oid)->CreateTable(ns_oid,
                                                                                    "temp_table_" + std::to_string((uint32_t)ns_oid), temp_schema);


  if (temp_table_oid == catalog::INVALID_TABLE_OID) {
    // Failed to create new namespace. Could be a concurrent DDL change and worth retrying
    txn_manager_->Abort(txn);
    STORAGE_LOG_ERROR("catalog::INVALID_TABLE_OID");
    return catalog::INVALID_TABLE_OID;
  }

  auto sequence_temp_table = catalog_->CreateTempTable(temp_schema);
  catalog_->GetAccessor(common::ManagedPointer(txn), db_oid)->SetTablePointer(temp_table_oid, sequence_temp_table);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return temp_table_oid;
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
