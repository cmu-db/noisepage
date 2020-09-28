#include "network/connection_handler_task.h"

#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id,
                                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory)
    : NotifiableTask(task_id), connection_handle_factory_(connection_handle_factory) {
  loop_ = ev_loop_new(EVFLAG_AUTO);

  // This callback function just calls HandleDispatch().
  async_callback handle_dispatch = [](struct ev_loop *, ev_async *event, int) {
    static_cast<ConnectionHandlerTask *>(event->data)->HandleDispatch();
  };

  // TODO gotta copy this ish to dispatcher for now
  terminate_ = reinterpret_cast<ev_async *>(malloc(sizeof(ev_async)));
  terminate_->data = loop_;
  ev_async_init(terminate_, [](struct ev_loop *, struct ev_async *event, int) {
    ev_break(static_cast<struct ev_loop *>(event->data), EVBREAK_ALL);
  });
  ev_async_start(loop_, terminate_);

  // Register an event that needs to be explicitly activated. When the event is handled, HandleDispatch() is called.
  notify_event_ = reinterpret_cast<ev_async *>(malloc(sizeof(ev_async)));
  notify_event_->data = this;
  ev_async_init(notify_event_, handle_dispatch);
  ev_async_start(loop_, notify_event_);
  //notify_event_ = RegisterEvent(EventUtil::EVENT_ACTIVATE_OR_TIMEOUT_ONLY, EV_READ | EV_PERSIST, handle_dispatch, this);
}

ConnectionHandlerTask::~ConnectionHandlerTask() {
  ev_async_stop(loop_, notify_event_);
  free(notify_event_);
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
  //event_active(notify_event_, 0 /* dummy arg */, 0 /* dummy arg */);
  ev_async_send(loop_, notify_event_);
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
