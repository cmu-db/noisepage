#pragma once

#include <memory>

#include "spdlog/logger.h"

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

namespace spdlog::details {
struct console_stdout;
struct console_mutex;
}  // namespace spdlog::details

namespace spdlog::sinks {
template <typename TargetStream, typename ConsoleMutex>
class stdout_sink;

using stdout_sink_mt = stdout_sink<details::console_stdout, details::console_mutex>;
}  // namespace spdlog::sinks

extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;  // NOLINT

extern template void spdlog::logger::trace<std::string>(const std::string &);
extern template void spdlog::logger::debug<std::string>(const std::string &);
extern template void spdlog::logger::info<std::string>(const std::string &);
extern template void spdlog::logger::warn<std::string>(const std::string &);
extern template void spdlog::logger::error<std::string>(const std::string &);

extern template void spdlog::logger::trace<>(const char *fmt);
extern template void spdlog::logger::debug<>(const char *fmt);
extern template void spdlog::logger::info<>(const char *fmt);
extern template void spdlog::logger::warn<>(const char *fmt);
extern template void spdlog::logger::error<>(const char *fmt);

extern template void spdlog::logger::trace<std::string_view>(const std::string_view &);
extern template void spdlog::logger::debug<std::string_view>(const std::string_view &);
extern template void spdlog::logger::info<std::string_view>(const std::string_view &);
extern template void spdlog::logger::warn<std::string_view>(const std::string_view &);
extern template void spdlog::logger::error<std::string_view>(const std::string_view &);

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
