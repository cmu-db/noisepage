#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/stat_registry.h"
#include "network/terrier_server.h"
#include "settings/settings_param.h"
#include "transaction/transaction_manager.h"

namespace terrier {

namespace settings {
class SettingsManager;
class SettingsTests;
}  // namespace settings

/**
 * The DBMain Class holds all the static functions, singleton pointers, etc.
 * It has the full knowledge of the whole database systems.
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

  /**
   * This function boots the backend components.
   * It initializes the following components in the following order:
   *    SettingsManager
   *    Garbage Collector
   *    Catalog
   *    Worker Pool
   *    Logging
   *    Stats
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

  // TODO(Weichen): Use unique ptr is enough.
  /**
   * Basic empty callbacks used by settings manager
   * @param old_value the old value of corresponding setting
   * @param new_value the new value of corresponding setting
   * @param action_context action context for empty callback
   */
  void EmptyCallback(void *old_value, void *new_value, const std::shared_ptr<common::ActionContext> &action_context);

  /**
   * Buffer pool size callback used by settings manager
   * @param old_value old value of buffer pool size
   * @param new_value new value of buffer pool size
   * @param action_context action context for changing buffer pool size
   */
  void BufferPoolSizeCallback(void *old_value, void *new_value,
                              const std::shared_ptr<common::ActionContext> &action_context);

 private:
  friend class settings::SettingsManager;
  friend class settings::SettingsTests;
  std::shared_ptr<common::StatisticsRegistry> main_stat_reg_;
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
  transaction::TransactionManager *txn_manager_;
  catalog::Catalog *catalog_;
  settings::SettingsManager *settings_manager_;
  network::TerrierServer server_;

  /**
   * Initializes all loggers.
   * If you have a new logger to initialize, put it here.
   */
  void InitLoggers();

  /**
   * Cleans up and exit.
   */
  void CleanUp();
};

}  // namespace terrier
