#include "traffic_cop/traffic_cop.h"

#include <network/connection_context.h>

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "common/exception.h"
#include "network/postgres/postgres_packet_writer.h"
#include "parser/postgresparser.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

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
    // TODO(Matt): blocking callback here?
    txn_manager_->Commit(txn.Get(), connection_ctx->Callback(), connection_ctx->CallbackArg());
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

void TrafficCop::ExecuteSimpleQuery(const std::string &simple_query,
                                    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                    const common::ManagedPointer<network::PostgresPacketWriter> out) const {
  // Attempt to parse the statement first
  auto parse_result = TrafficCopUtil::Parse(simple_query);

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
       statement.CastManagedPointerTo<parser::TransactionStatement>()->GetTransactionType() !=
           parser::TransactionStatement::CommandType::kRollback)) {
    out->WriteErrorResponse("ERROR:  current transaction is aborted, commands ignored until end of transaction block");
    return;
  }

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

      return;
    }
    default: {
      out->WriteErrorResponse("ERROR:  unsupported statement type");
      return;
    }
  }
}

std::pair<catalog::db_oid_t, catalog::namespace_oid_t> TrafficCop::CreateTempNamespace(
    int sockfd, const std::string &database_name) {
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name);

  if (db_oid == catalog::INVALID_DATABASE_OID) {
    txn_manager_->Abort(txn);
    return {catalog::INVALID_DATABASE_OID, catalog::INVALID_NAMESPACE_OID};
  }

  auto ns_oid = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid)
                    ->CreateNamespace(std::string(TEMP_NAMESPACE_PREFIX) + std::to_string(sockfd));
  if (ns_oid == catalog::INVALID_NAMESPACE_OID) {
    txn_manager_->Abort(txn);
    return {db_oid, catalog::INVALID_NAMESPACE_OID};
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return {db_oid, ns_oid};
}

bool TrafficCop::DropTempNamespace(catalog::namespace_oid_t ns_oid, catalog::db_oid_t db_oid) {
  auto txn = txn_manager_->BeginTransaction();
  auto db_accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
  if (!db_accessor) {
    txn_manager_->Abort(txn);
    return false;
  }

  auto result = db_accessor->DropNamespace(ns_oid);
  if (result) {
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  } else {
    txn_manager_->Abort(txn);
  }
  return result;
}

}  // namespace terrier::trafficcop
