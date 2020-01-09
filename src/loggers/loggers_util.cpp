#include "loggers/loggers_util.h"

#include <iostream>
#include <memory>

#include "loggers/binder_logger.h"
#include "loggers/catalog_logger.h"
#include "loggers/common_logger.h"
#include "loggers/execution_logger.h"
#include "loggers/index_logger.h"
#include "loggers/network_logger.h"
#include "loggers/optimizer_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/settings_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink = nullptr;  // NOLINT

namespace terrier {
void LoggersUtil::Initialize() {
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
    storage::InitIndexLogger();
    network::InitNetworkLogger();
    optimizer::InitOptimizerLogger();
    parser::InitParserLogger();
    settings::InitSettingsLogger();
    storage::InitStorageLogger();
    transaction::InitTransactionLogger();

    // Flush all *registered* loggers using a worker thread. Registered loggers must be thread safe for this to work
    // correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "Debug logging initialization failed for " << ex.what() << std::endl;  // NOLINT
    throw ex;
  }
}

void LoggersUtil::ShutDown() {
  if (default_sink != nullptr) {
    spdlog::shutdown();
    default_sink = nullptr;
  }
}
}  // namespace terrier
