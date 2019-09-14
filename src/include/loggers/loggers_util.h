#pragma once

#include <iostream>
#include "loggers/catalog_logger.h"
#include "loggers/execution_logger.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/network_logger.h"
#include "loggers/optimizer_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/settings_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/test_logger.h"
#include "loggers/transaction_logger.h"

namespace terrier {

/**
 * Debug loggers get initialized here in a single utility class.
 */
class LoggersUtil {
 public:
  LoggersUtil() = delete;

  /**
   * Initialize all of the debug loggers in the system.
   * @param testing true if the test logger should be initialized
   */
  static void Initialize(const bool testing) {
    try {
      InitMainLogger();
      // initialize namespace specific loggers
      storage::InitIndexLogger();
      storage::InitStorageLogger();
      transaction::InitTransactionLogger();
      catalog::InitCatalogLogger();
      optimizer::InitOptimizerLogger();
      settings::InitSettingsLogger();
      parser::InitParserLogger();
      network::InitNetworkLogger();
      execution::InitExecutionLogger();

      if (testing) {
        InitTestLogger();
      }

      // Flush all *registered* loggers using a worker thread. Registered loggers must be thread safe for this to work
      // correctly
      spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
    } catch (const spdlog::spdlog_ex &ex) {
      std::cerr << "Debug logging initialization failed for " << ex.what() << std::endl;  // NOLINT
      throw ex;
    }
    // log init now complete
    LOG_TRACE("Debug logging initialization complete.");
  }

  /**
   * Shut down all of the debug loggers in the system.
   */
  static void ShutDown() { spdlog::shutdown(); }
};
}  // namespace terrier
