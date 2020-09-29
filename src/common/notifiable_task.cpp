#include "common/notifiable_task.h"

#include "common/event_util.h"

namespace terrier::common {

NotifiableTask::NotifiableTask(ev::loop_ref loop, int task_id) : loop_(loop), task_id_(task_id) {
  terminate_ = new ev::async(loop_);
  terminate_->set<&NotifiableTask::TerminateCallback>(&loop_);
  terminate_->start();
}

NotifiableTask::~NotifiableTask() {
  for (ev::io *event : io_events_) {
    event->stop();
    delete event;
  }
  for (ev::async *event : async_events_) {
    event->stop();
    delete event;
  }
  terminate_->stop();
  delete terminate_;
  // I was not able to get the C++ API version of non-default loops working
  ev_loop_destroy(loop_);
}

void NotifiableTask::TerminateCallback(ev::async &event, int /*unused*/) {
  static_cast<ev::loop_ref *>(event.data)->break_loop(ev::ALL);
}

void NotifiableTask::UnregisterIoEvent(ev::io *event) { UnregisterEvent(event, io_events_); }

void NotifiableTask::UnregisterAsyncEvent(ev::async *event) { UnregisterEvent(event, async_events_); }

}  // namespace terrier::common
