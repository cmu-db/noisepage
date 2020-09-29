#include "common/io_timeout_event.h"

namespace terrier::common {

IoTimeoutEvent::IoTimeoutEvent(ev::loop_ref loop) { set(loop); }

void IoTimeoutEvent::set(ev::loop_ref loop) {
  io_event_.set(loop);
  timer_event_.set(loop);
  timer_event_.data = &io_event_;
}
void IoTimeoutEvent::start(int fd, uint16_t flags, ev_tstamp after) {
  io_event_.start(fd, flags);
  if (after != WAIT_FOREVER) {
    timer_event_.start(after);
  }
}

void IoTimeoutEvent::start() {
  // TODO(Joe) unclear when this is called and how to handle timeouts here
  io_event_.start();
}

void IoTimeoutEvent::stop() {
  io_event_.stop();
  timer_event_.stop();
}

}  // namespace terrier::common