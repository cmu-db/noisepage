#pragma once

#include <memory>

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

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

/**
 * Utility class that ties the life cycle of the loggers to it. This is useful when relying on destructor ordering to
 * shut components down because this can trigger a ShutDown() call after a class' destructor would be invoked.
 */
class LoggersHandle {
 public:
  /**
   * Initialize the loggers on instantiation.
   */
  LoggersHandle() { LoggersUtil::Initialize(); }

  /**
   * Shut the loggers down on destruction.
   */
  ~LoggersHandle() { LoggersUtil::ShutDown(); }
};
}  // namespace terrier
