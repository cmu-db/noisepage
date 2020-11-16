#include "loggers/loggers_util.h"

#include <iostream>
#include <memory>

#include "loggers/binder_logger.h"
#include "loggers/catalog_logger.h"
#include "loggers/common_logger.h"
#include "loggers/execution_logger.h"
#include "loggers/index_logger.h"
#include "loggers/messenger_logger.h"
#include "loggers/metrics_logger.h"
#include "loggers/network_logger.h"
#include "loggers/optimizer_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/settings_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink = nullptr;  // NOLINT
#endif

namespace noisepage {
void LoggersUtil::Initialize() {
#ifdef NOISEPAGE_USE_LOGGING
  try {
    // create the default, shared sink
    if (default_sink == nullptr) {
      default_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();  // NOLINT
    }
    // initialize namespace specific loggers
    binder::InitBinderLogger();
    catalog::InitCatalogLogger();
    common::InitCommonLogger();
    execution::InitExecutionLogger();
    messenger::InitMessengerLogger();
    metrics::InitMetricsLogger();
    network::InitNetworkLogger();
    optimizer::InitOptimizerLogger();
    parser::InitParserLogger();
    settings::InitSettingsLogger();
    storage::InitIndexLogger();
    storage::InitStorageLogger();
    transaction::InitTransactionLogger();

    // Flush all *registered* loggers using a worker thread. Registered loggers must be thread safe for this to work
    // correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "Debug logging initialization failed for " << ex.what() << std::endl;  // NOLINT
    throw ex;
  }
#endif
}

void LoggersUtil::ShutDown() {
#ifdef NOISEPAGE_USE_LOGGING
  if (default_sink != nullptr) {
    spdlog::shutdown();
    default_sink = nullptr;
  }
#endif
}
}  // namespace noisepage
