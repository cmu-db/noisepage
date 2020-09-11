#include "network/connection_handler_task.h"

#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id,
                                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory)
    : NotifiableTask(task_id), connection_handle_factory_(connection_handle_factory) {
  notify_event_ = RegisterEvent(
      EventUtil::EVENT_ACTIVATE_OR_TIMEOUT_ONLY, EV_READ | EV_PERSIST,
      [](int fd, int16_t flags, void *arg) { static_cast<ConnectionHandlerTask *>(arg)->HandleDispatch(); }, this);
}

void ConnectionHandlerTask::Notify(int conn_fd, std::unique_ptr<ProtocolInterpreter> protocol_interpreter) {
  {
    // This latch prevents a race where one thread is calling Notify to add to the list of jobs, and
    // another thread is calling HandleDispatch to consume the list of jobs.
    common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
    jobs_.emplace_back(conn_fd, std::move(protocol_interpreter));
  }
  int flags = 0;       // Dummy arg.
  int16_t ncalls = 0;  // Dummy arg.
  event_active(notify_event_, flags, ncalls);
}

void ConnectionHandlerTask::HandleDispatch() {
  common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
  for (auto &job : jobs_) {
    auto task = common::ManagedPointer<ConnectionHandlerTask>(this);
    auto &handle = connection_handle_factory_->NewConnectionHandle(job.first, std::move(job.second), task);
    handle.RegisterToReceiveEvents();
  }
  jobs_.clear();
}

}  // namespace terrier::network
