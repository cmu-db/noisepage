#include "network/connection_handler_task.h"

#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id,
                                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory)
    : NotifiableTask(new ev::dynamic_loop, task_id), connection_handle_factory_(connection_handle_factory) {
  // Register an event that needs to be explicitly activated. When the event is handled, HandleDispatch() is called.
  notify_event_ = new ev::async(*loop_);
  notify_event_->set<&ConnectionHandlerTask::HandleDispatchCallback>(this);
  notify_event_->start();
}

ConnectionHandlerTask::~ConnectionHandlerTask() {
  notify_event_->stop();
  delete notify_event_;
}

void ConnectionHandlerTask::Notify(int conn_fd, std::unique_ptr<ProtocolInterpreter> protocol_interpreter) {
  // Add the new connection to the list of jobs to be handled.
  {
    // This latch prevents a race where one thread is calling Notify to add to the list of jobs, and
    // another thread is calling HandleDispatch to consume the list of jobs.
    common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
    jobs_.emplace_back(conn_fd, std::move(protocol_interpreter));
  }
  // Signal that there are jobs to be dispatched.
  notify_event_->send();
}

void ConnectionHandlerTask::HandleDispatchCallback(ev::async &event, int /*unused*/) {
  static_cast<ConnectionHandlerTask *>(event.data)->HandleDispatch();
}

void ConnectionHandlerTask::HandleDispatch() {
  common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
  // For each connection that needs to be handled, a new ConnectionHandle is created and marked as ready to receive.
  for (auto &job : jobs_) {
    auto task = common::ManagedPointer<ConnectionHandlerTask>(this);
    auto &handle = connection_handle_factory_->NewConnectionHandle(job.first, std::move(job.second), task);
    handle.RegisterToReceiveEvents();
  }
  jobs_.clear();
}

}  // namespace terrier::network
