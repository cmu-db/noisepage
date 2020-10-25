#pragma once

#include <functional>
#include <string_view>

#include "common/managed_pointer.h"

namespace noisepage::messenger {

class Messenger;

/**
 * All messages can take in a callback function to be invoked when a reply is received.
 */
using CallbackFn = std::function<void(common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                      std::string_view message, uint64_t recv_cb_id)>;

/** A noop version of CallbackFn. */
static inline void CallbackFnNoop(common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                  std::string_view message, uint64_t recv_cb_id) {}

}  // namespace noisepage::messenger