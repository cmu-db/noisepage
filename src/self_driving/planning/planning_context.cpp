#include "self_driving/planning/planning_context.h"

#include "catalog/catalog.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving::pilot {

void PlanningContext::AddDatabase(catalog::db_oid_t db_oid) {
  auto txn = txn_manager_->BeginTransaction();
  db_oids_.insert(db_oid);
  db_oid_to_accessor_.emplace(
      std::make_pair(db_oid, catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED)));
  db_oid_to_txn_.emplace(std::make_pair(db_oid, txn));
}

void PlanningContext::ClearDatabases() {
  // Abort transactions since we don't want to commit the changes during the planning
  for (auto &[db_oid, txn] : db_oid_to_txn_) txn_manager_->Abort(txn.Get());

  db_oids_.clear();
  db_oid_to_txn_.clear();
  db_oid_to_accessor_.clear();
}
}  // namespace noisepage::selfdriving::pilot
