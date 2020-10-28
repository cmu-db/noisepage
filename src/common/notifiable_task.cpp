#include "common/notifiable_task.h"

#include "common/error/exception.h"

namespace noisepage::common {

NotifiableTask::NotifiableTask(std::unique_ptr<ev::loop_ref> loop, int task_id) : loop_(std::move(loop)), task_id_(task_id) {
  if (*loop_ == nullptr) {
    throw NETWORK_PROCESS_EXCEPTION("Unable to create event loop");
  }

  terminate_ = new ev::async(*loop_);
  terminate_->set<&NotifiableTask::TerminateCallback>(loop_.get());
  terminate_->start();
}

NotifiableTask::~NotifiableTask() {
  for (IoTimeoutEvent *event : io_events_) {
    event->Stop();
    delete event;
  }
  for (ev::async *event : async_events_) {
    event->stop();
    delete event;
  }
  terminate_->stop();
  delete terminate_;
  ev_loop_destroy(*loop_);
}

void NotifiableTask::TerminateCallback(ev::async &event, int /*unused*/) {
  static_cast<ev::loop_ref *>(event.data)->break_loop(ev::ALL);
}

void NotifiableTask::UnregisterIoEvent(IoTimeoutEvent *event) {
  auto it = io_events_.find(event);
  if (it == io_events_.end()) return;
  event->Stop();
  io_events_.erase(event);
  delete event;
}

void NotifiableTask::UnregisterAsyncEvent(ev::async *event) {
  auto it = async_events_.find(event);
  if (it == async_events_.end()) return;
  event->stop();
  async_events_.erase(event);
  delete event;
}

}  // namespace noisepage::common
