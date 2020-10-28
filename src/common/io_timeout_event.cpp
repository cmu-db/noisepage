#include "common/io_timeout_event.h"

namespace noisepage::common {

IoTimeoutEvent::IoTimeoutEvent(ev::loop_ref loop) { Set(loop); }

void IoTimeoutEvent::Set(ev::loop_ref loop) {
  io_event_.set(loop);
  timer_event_.set(loop);
  timer_event_.data = &io_event_;
}
void IoTimeoutEvent::Start(int fd, uint16_t flags, ev_tstamp after) {
  io_event_.start(fd, flags);
  if (after != WAIT_FOREVER) {
    timer_event_.start(after);
  }
}

void IoTimeoutEvent::Start() {
  // TODO(Joe) unclear when this is called and how to handle timeouts here
  io_event_.start();
}

void IoTimeoutEvent::Stop() {
  io_event_.stop();
  timer_event_.stop();
}

}  // namespace noisepage::common
