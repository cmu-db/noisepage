#include "traffic_cop/traffic_cop.h"
#include <memory>
#include <string>
#include <utility>
#include "traffic_cop/traffic_cop_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::trafficcop {

void TrafficCop::HandBufferToReplication(std::unique_ptr<network::ReadBuffer> buffer) {
  TERRIER_ASSERT(replication_log_provider_ != DISABLED, "Should not be handing off logs if no log provider was given");
  replication_log_provider_->HandBufferToReplication(std::move(buffer));
}

std::pair<catalog::db_oid_t, catalog::namespace_oid_t> TrafficCop::CreateTempNamespace(
    int sockfd, const std::string &database_name) {
  auto txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->GetDatabaseOid(txn, database_name);

  if (db_oid == catalog::INVALID_DATABASE_OID) {
    txn_manager_->Abort(txn);
    return {catalog::INVALID_DATABASE_OID, catalog::INVALID_NAMESPACE_OID};
  }

  auto ns_oid =
      catalog_->GetAccessor(txn, db_oid)->CreateNamespace(std::string(TEMP_NAMESPACE_PREFIX) + std::to_string(sockfd));
  if (ns_oid == catalog::INVALID_NAMESPACE_OID) {
    txn_manager_->Abort(txn);
    return {db_oid, catalog::INVALID_NAMESPACE_OID};
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return {db_oid, ns_oid};
}

bool TrafficCop::DropTempNamespace(catalog::namespace_oid_t ns_oid, catalog::db_oid_t db_oid) {
  auto txn = txn_manager_->BeginTransaction();
  auto db_accessor = catalog_->GetAccessor(txn, db_oid, ns_oid);
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
