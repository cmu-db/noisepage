#pragma once
#include <utility>
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
   * This function initializes the following components in order:
   *    SettingsManager
   *    Garbage Collector
   *    Catalog
   *    Worker Pool
   *    Logging
   *    Stats
   */
  void Init(int argc, char **argv);

  /**
   * This function boots traffic cop and networking layer.
   * It will block until server shuts down.
   * @return 0 if started successfully; 1 otherwise
   */
  void Run();

  /**
   * Shuts down the server.
   * It is worth noting that in normal cases, terrier will shut down and return from Run().
   * Use this function only when you want to force shutdown the server.
   * For example, in the end of unit tests when you want to shut down your test server.
   */
  void ForceShutdown();

 private:
  // friend class SettingsManager
  std::shared_ptr<common::StatisticsRegistry> main_stat_reg_;
  network::TerrierServer terrier_server_;

  void InitLoggers();

  void CleanUp();
};
}  // namespace terrier
