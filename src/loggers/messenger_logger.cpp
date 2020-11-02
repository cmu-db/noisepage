#include "loggers/messenger_logger.h"

#include <memory>

<<<<<<< HEAD
namespace terrier::messenger {

=======
namespace noisepage::messenger {
#ifdef NOISEPAGE_USE_LOGGING
>>>>>>> 193244ce13033c9e65563c6e7d0ccdedd0eaf973
std::shared_ptr<spdlog::logger> messenger_logger = nullptr;  // NOLINT

void InitMessengerLogger() {
  if (messenger_logger == nullptr) {
    messenger_logger = std::make_shared<spdlog::logger>("messenger_logger", ::default_sink);  // NOLINT
    spdlog::register_logger(messenger_logger);
  }
}
<<<<<<< HEAD

}  // namespace terrier::messenger
=======
#endif
}  // namespace noisepage::messenger
>>>>>>> 193244ce13033c9e65563c6e7d0ccdedd0eaf973
