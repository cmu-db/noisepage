#pragma once

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

#include "common/sanctioned_shared_pointer.h"
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

extern noisepage::common::SanctionedSharedPtr<spdlog::sinks::stdout_sink_mt>::Ptr default_sink;

namespace noisepage {

/**
 * Debug loggers get initialized here in a single static class.
 */
class LoggersUtil {
 public:
  LoggersUtil() = delete;

  /**
   * Initialize all of the debug loggers in the system.
   */
  static void Initialize();

  /**
   * Shut down all of the debug loggers in the system.
   */
  static void ShutDown();
};
}  // namespace noisepage
