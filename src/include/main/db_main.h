#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/stat_registry.h"
#include "common/worker_pool.h"
#include "network/terrier_server.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"

namespace terrier {

namespace settings {
class SettingsManager;
class SettingsTests;
class Callbacks;
}  // namespace settings

/**
 * The DBMain Class holds all the singleton pointers. It has the full knowledge
 * of the whole database systems and serves as a global context of the system.
 * *Only the settings manager should be able to access the DBMain object.*
 */
class DBMain {
 public:
  DBMain() = default;

  /**
   * The constructor of DBMain
   * @param param_map a map stores setting values
   */
  explicit DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
      : param_map_(std::move(param_map)) {}

  ~DBMain() {
    if (initialized) {
      ForceShutdown();
      delete gc_thread_;
      delete settings_manager_;
      delete catalog_;
      delete txn_manager_;
      delete buffer_segment_pool_;
      delete thread_pool_;
    }
  }

  /**
   * This function boots the backend components.
   * It initializes the following components in the following order:
   *    Debug loggers
   *    Stats registry (counters)
   *    Buffer segment pools
   *    Transaction manager
   *    Garbage collector thread
   *    Catalog
   *    Settings manager
   *    Worker pool
   */
  void Init();

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
  std::shared_ptr<common::StatisticsRegistry> main_stat_reg_;
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
  transaction::TransactionManager *txn_manager_;
  catalog::Catalog *catalog_;
  settings::SettingsManager *settings_manager_;
  storage::GarbageCollectorThread *gc_thread_;
  network::TerrierServer server_;
  storage::RecordBufferSegmentPool *buffer_segment_pool_;
  common::WorkerPool *thread_pool_;

  bool running = false;
  bool initialized = false;

  /**
   * Cleans up and exit.
   */
  void CleanUp();
};

}  // namespace terrier
