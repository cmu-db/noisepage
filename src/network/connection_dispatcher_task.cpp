#include "network/connection_dispatcher_task.h"

#include <csignal>
#include <memory>

#include "common/dedicated_thread_registry.h"

#define MASTER_THREAD_ID (-1)

namespace terrier::network {

ConnectionDispatcherTask::ConnectionDispatcherTask(
    uint32_t num_handlers, int listen_fd, common::DedicatedThreadOwner *dedicated_thread_owner,
    common::ManagedPointer<ProtocolInterpreter::Provider> interpreter_provider,
    common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
    common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry)
    : NotifiableTask(MASTER_THREAD_ID),
      num_handlers_(num_handlers),
      dedicated_thread_owner_(dedicated_thread_owner),
      connection_handle_factory_(connection_handle_factory),
      thread_registry_(thread_registry),
      interpreter_provider_(interpreter_provider),
      next_handler_(0) {
  RegisterEvent(listen_fd, EV_READ | EV_PERSIST, METHOD_AS_CALLBACK(ConnectionDispatcherTask, DispatchConnection),
                this);
  RegisterSignalEvent(SIGHUP, METHOD_AS_CALLBACK(NotifiableTask, ExitLoop), this);
}

// TODO(Gus): DispatchConnection will also need to take an argument telling it which interpreter
// to use for the connection.
void ConnectionDispatcherTask::DispatchConnection(int fd, int16_t) {  // NOLINT
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);

  int new_conn_fd = accept(fd, reinterpret_cast<struct sockaddr *>(&addr), &addrlen);
  if (new_conn_fd == -1) {
    NETWORK_LOG_ERROR("Failed to accept");
    return;
  }

  // Dispatch by rand number
  uint64_t handler_id = next_handler_;

  // update next threadID
  next_handler_ = (next_handler_ + 1) % handlers_.size();

  auto handler = handlers_[handler_id];
  NETWORK_LOG_TRACE("Dispatching connection to worker {0}", handler_id);

  handler->Notify(new_conn_fd, interpreter_provider_->Get());
}

void ConnectionDispatcherTask::RunTask() {
  // create all of the ConnectionHandlerTasks, using the same DedicatedThreadOwner as this task's
  for (int task_id = 0; static_cast<uint32_t>(task_id) < num_handlers_; task_id++) {
    auto handler = thread_registry_->RegisterDedicatedThread<ConnectionHandlerTask>(dedicated_thread_owner_, task_id,
                                                                                    connection_handle_factory_);
    handlers_.push_back(handler);
  }
  EventLoop();
}

void ConnectionDispatcherTask::Terminate() {
  ExitLoop();
  // clean up the ConnectionHandlerTasks
  for (const auto &handler_task : handlers_) {
    const bool result UNUSED_ATTRIBUTE = thread_registry_->StopTask(
        dedicated_thread_owner_, handler_task.CastManagedPointerTo<common::DedicatedThreadTask>());
    TERRIER_ASSERT(result, "Failed to stop ConnectionHandlerTask.");
  }
}

}  // namespace terrier::network
