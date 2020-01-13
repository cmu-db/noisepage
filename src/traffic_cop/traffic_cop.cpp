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
  TERRIER_ASSERT(connection_ctx->Transaction() == nullptr,
                 "Attempting to begin on a ConnectionContext that already has a running txn.");
  const auto txn = txn_manager_->BeginTransaction();
  connection_ctx->SetTransaction(common::ManagedPointer(txn));
  connection_ctx->SetAccessor(catalog_->GetAccessor(common::ManagedPointer(txn), connection_ctx->GetDatabaseOid()));
}

void TrafficCop::CommitTransaction(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                   const network::NetworkCallback &callback) const {
  const auto txn = connection_ctx->Transaction();
  TERRIER_ASSERT(txn != nullptr, "Attempting to commit on a ConnectionContext that doesn't have a running txn.");
  txn_manager_->Commit(txn.Get(), callback, nullptr);
  connection_ctx->SetTransaction(nullptr);
  connection_ctx->SetAccessor(nullptr);
}

void TrafficCop::AbortTransaction(const common::ManagedPointer<network::ConnectionContext> connection_ctx) const {
  const auto txn = connection_ctx->Transaction();
  TERRIER_ASSERT(txn != nullptr, "Attempting to abort on a ConnectionContext that doesn't have a running txn.");
  txn_manager_->Abort(txn.Get());
  connection_ctx->SetTransaction(nullptr);
  connection_ctx->SetAccessor(nullptr);
}

void TrafficCop::HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
  TERRIER_ASSERT(replication_log_provider_ != DISABLED, "Should not be handing off logs if no log provider was given");
  replication_log_provider_->HandBufferToReplication(std::move(buffer));
}

void TrafficCop::ExecuteSimpleQuery(const std::string &simple_query,
                                    const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                    const common::ManagedPointer<network::PostgresPacketWriter> out,
                                    const network::NetworkCallback &callback) const {
  auto parse_result = TrafficCopUtil::Parse(simple_query);

  TERRIER_ASSERT(parse_result->GetStatements().size() <= 1,
                 "We currently expect one statement per string (psql and oltpbench).");

  // TODO(Matt:) some clients may send multiple statements in a single simple query packet/string. That behavior would
  // exist here, presumable looping over all of the elements in the ParseResult

  if (parse_result->Empty()) {
    out->WriteEmptyQueryResponse();
    return;
  }

  const auto statement = parse_result->GetStatement(0);
  const auto txn = connection_ctx->Transaction();

  if (txn != nullptr && txn->MustAbort()) {
    // ERROR:  current transaction is aborted, commands ignored until end of transaction block
    return;
  }

  switch (statement->GetType()) {
    case parser::StatementType::TRANSACTION: {
      // It's a BEGIN, COMMIT, or ROLLBACK
      auto txn_statement = statement.CastManagedPointerTo<parser::TransactionStatement>();
      switch (txn_statement->GetTransactionType()) {
        case parser::TransactionStatement::kBegin: {
          if (txn != nullptr) {
            // already in a transaction, postgres returns a warning
            return;
          }
          BeginTransaction(connection_ctx);
          // output BEGIN?
          return;
        }
        case parser::TransactionStatement::kCommit: {
          if (txn == nullptr) {
            // not in a transaction, postgres returns a warning and COMMIT
            return;
          }
          if (txn->MustAbort()) {
            // postgres returns ROLLBACK
            AbortTransaction(connection_ctx);
            return;
          }
          CommitTransaction(connection_ctx, callback);
          return;
        }
        case parser::TransactionStatement::kRollback: {
          if (txn == nullptr) {
            // not in a transaction, postgres returns a warning and ROLLBACK
            return;
          }
          AbortTransaction(connection_ctx);
          return;
        }
      }
    }
    case parser::StatementType::SELECT:
    case parser::StatementType::INSERT:
    case parser::StatementType::UPDATE:
    case parser::StatementType::DELETE:
    case parser::StatementType::CREATE:
    case parser::StatementType::DROP: {
      const bool single_statement_txn = connection_ctx->Transaction() == nullptr;

      if (single_statement_txn) {
        BeginTransaction(connection_ctx);
      }

      return;
    }
    default: {
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

  txn_manager_->Commit(
      txn, []() {}, nullptr);
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
    txn_manager_->Commit(
        txn, []() {}, nullptr);
  } else {
    txn_manager_->Abort(txn);
  }
  return result;
}

}  // namespace terrier::trafficcop
