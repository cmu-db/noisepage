#pragma once

#include <ev++.h>

namespace terrier::common {

class IoTimeoutEvent {
 public:
  static constexpr const ev_tstamp WAIT_FOREVER = -1.0;

  explicit IoTimeoutEvent(ev::loop_ref loop);

  void set(ev::loop_ref loop);

  template <void (*function)(ev::io &event, int)>
  void set(void *data = nullptr) {
    io_event_.set<function>(data);
    timer_event_.set<&TimeoutCallback>(&io_event_);
  }

  void start();

  void start(int fd, uint16_t flags, ev_tstamp after);

  void stop();

 private:
  static void TimeoutCallback(ev::timer &event, int flags) {
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

}  // namespace terrier::common