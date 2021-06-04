#include "self_driving/planning/planning_context.h"

#include "catalog/catalog.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving::pilot {

PlanningContext::PlanningContext(std::string ou_model_save_path, std::string interference_model_save_path,
                                 common::ManagedPointer<catalog::Catalog> catalog,
                                 common::ManagedPointer<metrics::MetricsThread> metrics_thread,
                                 common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
                                 common::ManagedPointer<settings::SettingsManager> settings_manager,
                                 common::ManagedPointer<optimizer::StatsStorage> stats_storage,
                                 common::ManagedPointer<transaction::TransactionManager> txn_manager,
                                 std::unique_ptr<util::QueryExecUtil> query_exec_util,
                                 common::ManagedPointer<task::TaskManager> task_manager)
    : ou_model_save_path_(std::move(ou_model_save_path)),
      interference_model_save_path_(std::move(interference_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      query_exec_util_(std::move(query_exec_util)),
      task_manager_(task_manager) {}

PlanningContext::~PlanningContext() = default;

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
