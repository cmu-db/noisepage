#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "self_driving/modeling/operating_unit_defs.h"
#include "self_driving/planning/memory_info.h"
#include "util/query_exec_util.h"

namespace noisepage {

namespace catalog {
class Catalog;
class CatalogAccessor;
}  // namespace catalog

namespace metrics {
class MetricsThread;
}

namespace modelserver {
class ModelServerManager;
}

namespace optimizer {
class StatsStorage;
}

namespace settings {
class SettingsManager;
}

namespace transaction {
class TransactionManager;
class TransactionContext;
}  // namespace transaction

namespace task {
class TaskManager;
}

}  // namespace noisepage

namespace noisepage::selfdriving::pilot {

/**
 * Planning context carries the necessary information and dependencies used in planning
 */
class PlanningContext {
 public:
  /**
   * Constructor for PlanningContext
   * @param ou_model_save_path OU model save path
   * @param interference_model_save_path interference model save path
   * @param catalog catalog
   * @param metrics_thread metrics thread for metrics manager
   * @param model_server_manager model server manager
   * @param settings_manager settings manager
   * @param stats_storage stats_storage
   * @param txn_manager transaction manager
   * @param query_exec_util query execution utility for the pilot to use
   * @param task_manager task manager to submit internal jobs to
   */
  PlanningContext(std::string ou_model_save_path, std::string interference_model_save_path,
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

  /** @brief Getter for ou_model_save_path */
  const std::string &GetOuModelSavePath() const { return ou_model_save_path_; }

  /** @brief Getter for interference_model_save_path_*/
  const std::string &GetInterferenceModelSavePath() const { return interference_model_save_path_; }

  /** @brief Getter for catalog_ */
  common::ManagedPointer<catalog::Catalog> GetCatalog() const { return catalog_; }

  /** @brief Getter for metrics_thread_ */
  common::ManagedPointer<metrics::MetricsThread> GetMetricsThread() const { return metrics_thread_; }

  /** @brief Getter for model_server_manager_ */
  common::ManagedPointer<modelserver::ModelServerManager> GetModelServerManager() const {
    return model_server_manager_;
  }

  /** @brief Getter for settings_manager_ */
  common::ManagedPointer<settings::SettingsManager> GetSettingsManager() const { return settings_manager_; }

  /** @brief Getter for stats_storage_ */
  common::ManagedPointer<optimizer::StatsStorage> GetStatsStorage() const { return stats_storage_; }

  /** @brief Getter for txn_manager_ */
  common::ManagedPointer<transaction::TransactionManager> GetTxnManager() const { return txn_manager_; }

  /** @brief Getter for query_exec_util_ */
  const std::unique_ptr<util::QueryExecUtil> &GetQueryExecUtil() const { return query_exec_util_; }

  /** @brief Getter for task_manager_ */
  common::ManagedPointer<task::TaskManager> GetTaskManager() const { return task_manager_; }

  /** @brief Getter for memory_info_ */
  const MemoryInfo &GetMemoryInfo() const { return memory_info_; }

  /** @brief Getter for db_oids_ */
  const std::set<catalog::db_oid_t> &GetDBOids() const { return db_oids_; }

  /** @brief Setter for memory_info_ */
  void SetMemoryInfo(MemoryInfo &&memory_info) { memory_info_ = std::move(memory_info); }

  /**
   * Add a database to the PlanningContext and create the associated transaction context and catalog accessor
   * @param db_oid Database oid
   */
  void AddDatabase(catalog::db_oid_t db_oid);

  /**
   * Clear the information for all databases. Abort all transactions, and destroy all catalog accessors
   */
  void ClearDatabases();

  /**
   * Get the TransactionContext associated with a database
   * @param db_oid Database oid
   * @return TransactionContext for that database
   */
  common::ManagedPointer<transaction::TransactionContext> GetTxnContext(catalog::db_oid_t db_oid) const {
    NOISEPAGE_ASSERT(db_oid_to_txn_.find(db_oid) != db_oid_to_txn_.end(), "Cannot find TransactionContext");
    return db_oid_to_txn_.at(db_oid);
  }

  /**
   * Get the CatalogAccessor associated with a database
   * @param db_oid Database oid
   * @return CatalogAccessor for that database
   */
  common::ManagedPointer<catalog::CatalogAccessor> GetCatalogAccessor(catalog::db_oid_t db_oid) const {
    NOISEPAGE_ASSERT(db_oid_to_accessor_.find(db_oid) != db_oid_to_accessor_.end(), "Cannot find CatalogAccessor");
    return common::ManagedPointer(db_oid_to_accessor_.at(db_oid));
  }

  /**
   * Add an OU inference to the cache
   * @param ou_type Type of OU
   * @param feature Input feature
   * @param label Predicted labels to cache
   */
  void AddOUInference(ExecutionOperatingUnitType ou_type, const std::vector<int> &feature, std::vector<double> label) {
    ou_inference_cache_.try_emplace(ou_type);
    ou_inference_cache_[ou_type][feature] = std::make_unique<std::vector<double>>(std::move(label));
  }

  /**
   * Check whether an OU inference is cached
   * @param ou_type Type of OU
   * @param feature Input feature
   * @return true if inference is caches; false otherwise
   */
  bool HasOUInference(ExecutionOperatingUnitType ou_type, const std::vector<int> &feature) const {
    if (ou_inference_cache_.find(ou_type) == ou_inference_cache_.end()) return false;
    if (ou_inference_cache_.at(ou_type).find(feature) == ou_inference_cache_.at(ou_type).end()) return false;
    return true;
  }

  /**
   * Acquire labels from OU inference cache (assuming the cache exists. Use HasOUInference() first)
   * @param ou_type Type of OU
   * @param feature Input feature
   * @return cached labels
   */
  const std::vector<double> *GetOUInference(ExecutionOperatingUnitType ou_type, const std::vector<int> &feature) const {
    NOISEPAGE_ASSERT(ou_inference_cache_.find(ou_type) != ou_inference_cache_.end(), "Cannot find OU type");
    NOISEPAGE_ASSERT(
        ou_inference_cache_.find(ou_type)->second.find(feature) != ou_inference_cache_.find(ou_type)->second.end(),
        "Cannot find feature");
    return ou_inference_cache_.at(ou_type).at(feature).get();
  }

  /**
   * Add an interference inference to the cache
   * @param feature Input feature
   * @param label Predicted labels to cache
   */
  void AddInterferenceInference(const std::vector<int> &feature, std::vector<double> label) {
    interference_inference_cache_[feature] = std::make_unique<std::vector<double>>(std::move(label));
  }

  /**
   * Check whether an interference inference is cached
   * @param feature Input feature
   * @return true if inference is caches; false otherwise
   */
  bool HasInterferenceInference(const std::vector<int> &feature) const {
    return interference_inference_cache_.find(feature) != interference_inference_cache_.end();
  }

  /**
   * Acquire labels from interference inference cache (assuming the cache exists. Use HasInterferenceInference() first)
   * @param feature Input feature
   * @return cached labels
   */
  const std::vector<double> *GetInterferenceInference(const std::vector<int> &feature) const {
    NOISEPAGE_ASSERT(interference_inference_cache_.find(feature) != interference_inference_cache_.end(),
                     "Cannot find feature");
    return interference_inference_cache_.at(feature).get();
  }

 private:
  const std::string ou_model_save_path_;
  const std::string interference_model_save_path_;
  const common::ManagedPointer<catalog::Catalog> catalog_;
  const common::ManagedPointer<metrics::MetricsThread> metrics_thread_;
  const common::ManagedPointer<modelserver::ModelServerManager> model_server_manager_;
  const common::ManagedPointer<settings::SettingsManager> settings_manager_;
  const common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  const common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  const std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  const common::ManagedPointer<task::TaskManager> task_manager_;
  pilot::MemoryInfo memory_info_;
  std::set<catalog::db_oid_t> db_oids_;
  std::unordered_map<catalog::db_oid_t, common::ManagedPointer<transaction::TransactionContext>> db_oid_to_txn_;
  std::unordered_map<catalog::db_oid_t, std::unique_ptr<catalog::CatalogAccessor>> db_oid_to_accessor_;
  // <ou, <feature, cached label>>
  std::map<ExecutionOperatingUnitType, std::map<std::vector<int>, std::unique_ptr<std::vector<double>>>>
      ou_inference_cache_;
  // <feature, cached label>
  std::map<std::vector<int>, std::unique_ptr<std::vector<double>>> interference_inference_cache_;
};

}  // namespace noisepage::selfdriving::pilot
