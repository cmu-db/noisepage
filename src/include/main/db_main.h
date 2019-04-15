#pragma once
#include <utility>
#include <memory>
#include "common/stat_registry.h"
#include "network/terrier_server.h"

namespace terrier {

/**
 * A DBMain object holds every thing (i.e. pointers to every component)
 * about the database system.
 * *Only the settings manager should be able to access the DBMain object.*
 */
class DBMain {
 public:
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
  void Init(int argc, char **argv);

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
  // friend class SettingsManager
  std::shared_ptr<common::StatisticsRegistry> main_stat_reg_;
  network::TerrierServer terrier_server_;

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
