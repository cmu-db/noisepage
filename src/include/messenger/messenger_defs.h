#pragma once

#include <functional>
#include <string_view>

#include "common/managed_pointer.h"

namespace noisepage::messenger {

class Messenger;
class ZmqMessage;

/**
 * All messages can take in a callback function to be invoked when a reply is received.
 */
using CallbackFn = std::function<void(common::ManagedPointer<Messenger> messenger, const ZmqMessage &msg)>;

/** Predefined convenience callbacks. */
class CallbackFns {
 public:
  /** A noop version of CallbackFn, provided for convenience. */
  static void Noop(common::ManagedPointer<Messenger> messenger, const ZmqMessage &msg) {}
};

}  // namespace noisepage::messenger
