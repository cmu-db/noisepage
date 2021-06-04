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
#include "loggers/model_server_logger.h"
#include "loggers/network_logger.h"
#include "loggers/optimizer_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/replication_logger.h"
#include "loggers/selfdriving_logger.h"
#include "loggers/settings_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

#ifdef NOISEPAGE_USE_LOGGING
noisepage::common::SanctionedSharedPtr<spdlog::sinks::stdout_sink_mt>::Ptr default_sink = nullptr;
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
    modelserver::InitModelServerLogger();
    optimizer::InitOptimizerLogger();
    parser::InitParserLogger();
    replication::InitReplicationLogger();
    selfdriving::InitSelfDrivingLogger();
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

std::optional<spdlog::level::level_enum> LoggersUtil::GetLevel(const std::string_view &name) {
  std::optional<spdlog::level::level_enum> level_val{std::nullopt};
  if (name == "off") {
    level_val = spdlog::level::off;
  } else if (name == "trace") {
    level_val = spdlog::level::trace;
  } else if (name == "debug") {
    level_val = spdlog::level::debug;
  } else if (name == "info") {
    level_val = spdlog::level::info;
  } else if (name == "err") {
    level_val = spdlog::level::err;
  } else if (name == "critical") {
    level_val = spdlog::level::critical;
  }
  return level_val;
}

common::SanctionedSharedPtr<spdlog::logger>::Ptr LoggersUtil::GetLogger(const std::string_view &name) {
  common::SanctionedSharedPtr<spdlog::logger>::Ptr logger = nullptr;
#ifdef NOISEPAGE_USE_LOGGING
  if (name == "binder") {
    logger = binder::binder_logger;
  } else if (name == "catalog") {
    logger = catalog::catalog_logger;
  } else if (name == "common") {
    logger = common::common_logger;
  } else if (name == "execution") {
    logger = execution::execution_logger;
  } else if (name == "index") {
    logger = storage::index_logger;
  } else if (name == "messenger") {
    logger = messenger::messenger_logger;
  } else if (name == "metrics") {
    logger = metrics::metrics_logger;
  } else if (name == "modelserver") {
    logger = modelserver::model_server_logger;
  } else if (name == "network") {
    logger = network::network_logger;
  } else if (name == "optimizer") {
    logger = optimizer::optimizer_logger;
  } else if (name == "parser") {
    logger = parser::parser_logger;
  } else if (name == "replication") {
    logger = replication::replication_logger;
  } else if (name == "selfdriving") {
    logger = selfdriving::selfdriving_logger;
  } else if (name == "settings") {
    logger = settings::settings_logger;
  } else if (name == "storage") {
    logger = storage::storage_logger;
  } else if (name == "transaction") {
    logger = transaction::transaction_logger;
  }
#endif
  return logger;
}

}  // namespace noisepage
