#pragma once

#include <ev++.h>

namespace noisepage::common {

/**
 * @brief IoTimeout is an event that can be triggered by some I/O event or a timeout
 *
 * IoTimeoutEvent creates a composite event from ev::io and ev::timer from libev to create an I/O event that will
 * timeout
 */
class IoTimeoutEvent {
 public:
  /** Timeout value to indicate that this event will never timeout */
  static constexpr const ev_tstamp WAIT_FOREVER = -1.0;

  /**
   * @brief Creates a new instance of IoTimeoutEvent
   *
   * @param loop event loop to create this event with
   */
  explicit IoTimeoutEvent(ev::loop_ref loop);

  /**
   * @brief set's the event loop of this event
   *
   * @param loop event loop to set for this event
   */
  void Set(ev::loop_ref loop);

  /**
   * @brief Set's the callback function for this event with some argument
   *
   * If the I/O event is fired then the callback function will be called with the I/O events flags.
   * If the timer event is fired then the I/O event is stopped and the callback function will be called with the
   * timer event's flags.
   * It is up to the callback function to check the flags to see if the event timed out or fired from an I/O event, if
   * they care.
   *
   * It is possible, though unlikely, that the timer event fires and right after the I/O event fires. This is due to how
   * libev checks for events. This will result in the callback function being called twice with each of the event's
   * flags.
   *
   * It should not be possible for the I/O event to be fired right after the timer event because stop should stop the event
   * even if it's in pending status.
   *
   * @tparam function function to set as a callback
   * @param data argument for the function callback
   */
  template <void (*function)(ev::io &event, int)>  // NOLINT
  void Set(void *data = nullptr) {
    io_event_.set<function>(data);
    timer_event_.set<&TimeoutCallback>(&io_event_);
  }

  /**
   * @brief starts the event
   */
  void Start();

  /**
   * @brief starts the event
   *
   * @param fd file descriptor to watch
   * @param flags type of I/O events to watch for (can be ev::READ or ev::WRITE)
   * @param after timeout in seconds for the event, set as WAIT_FOREVER to wait forever
   */
  void Start(int fd, uint16_t flags, ev_tstamp after);

  /**
   * Stop the event
   */
  void Stop();

 private:
  static void TimeoutCallback(ev::timer &event, int flags) {  // NOLINT
    auto *io_event = static_cast<ev::io *>(event.data);
    io_event->stop();
    /*
     * This will call the I/O event callback with a timeout flag so it can be properly handled as a timeout.
     * It's up to the call back function to properly detect if the event fired normally or if it timed out.
     * A possible alternative is to have two separate callback functions, one for a timeout and one for a
     * normal I/O firing
     */
    io_event->cb(io_event->loop, io_event, flags);
  }

  ev::io io_event_;
  ev::timer timer_event_;
};

}  // namespace noisepage::common
