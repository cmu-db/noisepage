#pragma once

#include <functional>
#include <string_view>

#include "common/managed_pointer.h"
#include "common/strong_typedef.h"

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

/** The ID of a messenger callback. */
STRONG_TYPEDEF_HEADER(callback_id_t, uint64_t);
/** The ID of a messenger outgoing connection. */
STRONG_TYPEDEF_HEADER(connection_id_t, uint64_t);
/** The ID of a messenger message. */
STRONG_TYPEDEF_HEADER(message_id_t, uint64_t);
/** The ID of a messenger incoming listening router. */
STRONG_TYPEDEF_HEADER(router_id_t, uint64_t);

}  // namespace noisepage::messenger
