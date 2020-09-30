#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include "common/error/exception.h"

namespace terrier {

/**
 * Static utility class with wrappers for libevent functions.
 *
 * Wrapper functions are functions with the same signature and return
 * value as the c-style functions, but consist of an extra return value
 * checking. An exception is thrown instead if something is wrong. Wrappers
 * are great tools for using legacy code in a modern code base.
 */
class EventUtil {
 private:
  template <typename T>
  static bool NotNull(T *ptr) {
    return ptr != nullptr;
  }

  static bool IsZero(int arg) { return arg == 0; }

  static bool NonNegative(int arg) { return arg >= 0; }

  template <typename T>
  static T Wrap(T value, bool (*check)(T), const char *error_msg) {
    if (!check(value)) throw NETWORK_PROCESS_EXCEPTION(error_msg);
    return value;
  }

 public:
  /**
   * EVENT_ACTIVATE_OR_TIMEOUT_ONLY can be supplied to event_new() to indicate that the event can only be activated
   * by an explicit event_active or by a timeout firing.
   */
  static constexpr const int EVENT_ACTIVATE_OR_TIMEOUT_ONLY = -1;
  /** WAIT_FOREVER can be supplied as a timeout value to indicate that the event should not timeout. */
  static constexpr const struct timeval *WAIT_FOREVER = nullptr;

  EventUtil() = delete;

  /**
   * @return A new event_base
   */
  static struct event_base *EventBaseNew() {
    return Wrap(event_base_new(), NotNull<struct event_base>, "Can't allocate event base");
  }

  /**
   * @param base The event_base to exit
   * @param timeout
   * @return a positive integer or an exception is thrown on failure
   */
  static int EventBaseLoopExit(struct event_base *base, const struct timeval *timeout) {
    return Wrap(event_base_loopexit(base, timeout), IsZero, "Error when exiting loop");
  }

  /**
   * @param event The event to delete
   * @return a positive integer or an exception is thrown on failure
   */
  static int EventDel(struct event *event) { return Wrap(event_del(event), IsZero, "Error when deleting event"); }

  /**
   * @param event The event to add
   * @param timeout
   * @return a positive integer or an exception is thrown on failure
   */
  static int EventAdd(struct event *event, const struct timeval *timeout) {
    return Wrap(event_add(event, timeout), IsZero, "Error when adding event");
  }

  /**
   * @brief Allocates a callback event
   * @param event The event being assigned
   * @param base The event_base
   * @param fd The filedescriptor assigned to the event
   * @param flags Flags for the event
   * @param callback Callback function to fire when the event is triggered
   * @param arg Argument to pass to the callback function
   * @return a positive integer or an exception is thrown on failure
   */
  static int EventAssign(struct event *event, struct event_base *base, int fd, int16_t flags,
                         event_callback_fn callback, void *arg) {
    return Wrap(event_assign(event, base, fd, flags, callback, arg), IsZero, "Error when assigning event");
  }

  /**
   * @brief Runs event base dispatch loop
   * @param base The event_base to dispatch on
   * @return a positive integer or an exception is thrown on failure
   */
  static int EventBaseDispatch(struct event_base *base) {
    return Wrap(event_base_dispatch(base), NonNegative, "Error in event base dispatch");
  }
};
}  // namespace terrier
