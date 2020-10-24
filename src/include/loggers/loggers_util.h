#pragma once

#include <memory>

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;  // NOLINT

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
