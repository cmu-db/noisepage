#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "common/action_context.h"
#include "common/stat_registry.h"
#include "common/worker_pool.h"
#include "metrics/metrics_manager.h"
#include "network/terrier_server.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"

namespace terrier {

namespace common {
class DedicatedThreadRegistry;
}  // namespace common

namespace metrics {
class MetricsTests;
}  // namespace metrics

namespace settings {
class SettingsManager;
class SettingsTests;
class Callbacks;
}  // namespace settings

namespace storage {
class WriteAheadLoggingTests;
}  // namespace storage

namespace trafficcop {
class TerrierEngine;
}  // namespace trafficcop

namespace transaction {
class DeferredActionManager;
}  // namespace transaction

/**
 * The DBMain Class holds all the singleton pointers. It has the full knowledge
 * of the whole database systems and serves as a global context of the system.
 * *Only the settings manager should be able to access the DBMain object.*
 */
class DBMain {
 public:
  /**
   * The constructor of DBMain
   * This function boots the backend components.
   * It initializes the following components in the following order:
   *    Debug loggers
   *    Stats registry (counters)
   *    Buffer segment pools
   *    Transaction manager
   *    Garbage collector thread
   *    Catalog
   *    Settings manager
   *    Log manager
   *    Worker pool
   * @param param_map a map stores setting values
   */
  explicit DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map);

  ~DBMain();

  /**
   * Boots the traffic cop and networking layer, starts the server loop.
   * It will block until server shuts down.
   */
  void Run();

  /**
   * Shuts down the server.
   * It is worth noting that in normal cases, terrier will shut down and return from Run().
   * So, use this function only when you want to shutdown the server from code.
   * For example, in the end of unit tests when you want to shut down your test server.
   */
  void ForceShutdown();

 private:
  friend class settings::SettingsManager;
  friend class settings::SettingsTests;
  friend class settings::Callbacks;
  friend class metrics::MetricsTests;
  std::unique_ptr<common::StatisticsRegistry> main_stat_reg_;
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
  transaction::TimestampManager *timestamp_manager_;
  transaction::TransactionManager *txn_manager_;
  std::unique_ptr<transaction::DeferredActionManager> deferred_action_manager_;
  settings::SettingsManager *settings_manager_;
  storage::LogManager *log_manager_;
  storage::GarbageCollector *garbage_collector_;
  storage::GarbageCollectorThread *gc_thread_;
  network::TerrierServer *server_;
  storage::RecordBufferSegmentPool *buffer_segment_pool_;
  common::WorkerPool *thread_pool_;
  trafficcop::TrafficCop *t_cop_;
  network::PostgresCommandFactory *command_factory_;
  network::ConnectionHandleFactory *connection_handle_factory_;
  network::ProtocolInterpreter::Provider *provider_;
  metrics::MetricsManager *metrics_manager_;
  common::DedicatedThreadRegistry *thread_registry_;
  std::unique_ptr<catalog::Catalog> catalog_;
  std::unique_ptr<parser::PostgresParser> parser_;
  std::unique_ptr<trafficcop::TerrierEngine> terrier_engine_;
  catalog::db_oid_t default_database_oid_;
  storage::BlockStore block_store_{10000, 10000};

  bool running_ = false;

  /**
   * Cleans up and exit.
   */
  void CleanUp();
};

}  // namespace terrier
