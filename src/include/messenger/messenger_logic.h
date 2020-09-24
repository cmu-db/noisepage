#pragma once

#include <cstdint>
#include <string>

namespace terrier::messenger {

class Messenger;

/**
 * All the known callbacks in the system are represented by a single char at the start of the message.
 * If we run out of ASCII-printable characters, we can revisit this and make Callbacks multi-char.
 */
enum Callbacks : uint8_t { NOOP = 'N', PRINT = 'P' };

/** MessengerLogic handles the actual work of processing messages, invoking other system components, etc. */
class MessengerLogic {
 public:
  void ProcessMessage(std::string_view sender, std::string_view message);

 private:
};

}  // namespace terrier::messenger
