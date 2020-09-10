#include "network/connection_handler_task.h"

#include <memory>
#include <utility>

#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id,
                                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory)
    : NotifiableTask(task_id), connection_handle_factory_(connection_handle_factory) {
  notify_event_ =
      RegisterEvent(-1, EV_READ | EV_PERSIST, METHOD_AS_CALLBACK(ConnectionHandlerTask, HandleDispatch), this);
}

void ConnectionHandlerTask::Notify(int conn_fd, std::unique_ptr<ProtocolInterpreter> protocol_interpreter) {
  {
    /**
     * this latch is needed to avoid a race condition with HandleDispatch consuming this deque in another thread
     */
    common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
    jobs_.emplace_back(conn_fd, std::move(protocol_interpreter));
  }
  int res = 0;         // Flags, unused attribute in event_active
  int16_t ncalls = 0;  // Unused attribute in event_active
  event_active(notify_event_, res, ncalls);
}

void ConnectionHandlerTask::HandleDispatch(int, int16_t) {  // NOLINT as we don't use the flags arg nor the fd
  common::SpinLatch::ScopedSpinLatch guard(&jobs_latch_);
  for (auto &job : jobs_) {
    connection_handle_factory_->NewConnectionHandle(job.first, std::move(job.second), common::ManagedPointer(this))
        .RegisterToReceiveEvents();
  }
  jobs_.clear();
}

}  // namespace terrier::network
