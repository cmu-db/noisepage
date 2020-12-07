#include "loggers/selfdriving_logger.h"

#include <memory>

namespace noisepage::selfdriving {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> selfdriving_logger = nullptr;  // NOLINT

void InitMessengerLogger() {
  if (selfdriving_logger == nullptr) {
    selfdriving_logger = std::make_shared<spdlog::logger>("selfdriving_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(selfdriving_logger);
  }
}
#endif
}  // namespace noisepage::selfdriving
