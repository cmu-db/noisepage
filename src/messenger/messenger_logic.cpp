#include "messenger/messenger_logic.h"

#include "common/json.h"
#include "loggers/messenger_logger.h"
#include "messenger/messenger.h"

namespace terrier::messenger {

void MessengerLogic::ProcessMessage(std::string_view sender, std::string_view message) {
  TERRIER_ASSERT(!message.empty(), "Empty messages are currently forbidden. Is there a use case for this?");

  // TODO(WAN): Document protocol.
  // Process the message.
  const char *data = message.data();
  // The first byte indicates the function to be dispatched to.
  const char function = *data++;
  // The remaining bytes are the payload.
  const char *payload = data;

  // A suggestion for adding additional functions in the future.
  // Write a wrapper for all functions that parse out the remaining payload.

  switch (function) {
    case Callbacks::NOOP: {
      break;
    }
    case Callbacks::PRINT: {
      MESSENGER_LOG_INFO(fmt::format("RECV \"{}\": \"{}\"", sender, payload));
      break;
    }
  }
}

}  // namespace terrier::messenger
