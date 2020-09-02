#pragma once

#include <memory>

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

// clang-format off
// The order of these headers matters. clang-format will try to sort them.
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_sinks.h"
// clang-format on

extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;  // NOLINT

namespace terrier {

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
}  // namespace terrier
