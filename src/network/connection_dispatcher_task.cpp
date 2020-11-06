#include "network/connection_dispatcher_task.h"

#include <csignal>

#include "common/dedicated_thread_registry.h"
#include "loggers/network_logger.h"
#include "network/connection_handler_task.h"

namespace {
constexpr uint32_t MAIN_THREAD_ID = -1;
}  // namespace

namespace noisepage::network {

ConnectionDispatcherTask::ConnectionDispatcherTask(
    uint32_t num_handlers, common::DedicatedThreadOwner *dedicated_thread_owner,
    common::ManagedPointer<ProtocolInterpreterProvider> interpreter_provider,
    common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
    common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
    std::initializer_list<int> file_descriptors)
    : NotifiableTask(MAIN_THREAD_ID),
      num_handlers_(num_handlers),
      dedicated_thread_owner_(dedicated_thread_owner),
      connection_handle_factory_(connection_handle_factory),
      thread_registry_(thread_registry),
      interpreter_provider_(interpreter_provider),
      next_handler_(0) {
  NOISEPAGE_ASSERT(num_handlers_ > 0, "No workers that connections can be dispatched to.");

  // The libevent callback functions are defined here.
  // Note that libevent callback functions must have type (int fd, int16_t flags, void *arg) -> void.

  // This callback dispatches client connections at fd to a handler. This uses the dispatcher's protocol interpreter.
  event_callback_fn connection_dispatcher_fn = [](int fd, int16_t flags, void *arg) {
    auto *dispatcher = static_cast<ConnectionDispatcherTask *>(arg);
    dispatcher->DispatchConnection(fd, dispatcher->interpreter_provider_);
  };
  // This callback exits the event loop.
  event_callback_fn loop_exit_fn = [](int fd, int16_t flags, void *arg) {
    static_cast<NotifiableTask *>(arg)->ExitLoop(fd, flags);
  };

  // Specific events are then associated with their respective libevent callback functions.

  for (auto listen_fd : file_descriptors) {
    // Dispatch a new connection every time the file descriptor becomes readable again.
    //   EV_READ : Wait until the file descriptor becomes readable.
    //   EV_PERSIST : Non-persistent events are removed upon activation (single-use), the server should be persistent.
    RegisterEvent(listen_fd, EV_READ | EV_PERSIST, connection_dispatcher_fn, this);
  }
  // Exit the event loop if the terminal launching the server process is closed.
  RegisterSignalEvent(SIGHUP, loop_exit_fn, this);
}

void ConnectionDispatcherTask::DispatchConnection(uint32_t fd,
                                                  common::ManagedPointer<ProtocolInterpreterProvider> provider) {
  // Wait for a new socket connection. Currently, addr and addrlen are unused.
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);
  int new_conn_fd = accept(fd, reinterpret_cast<struct sockaddr *>(&addr), &addrlen);
  if (new_conn_fd == -1) {
    NETWORK_LOG_ERROR("Failed to accept a new connection: {}", strerror(errno));
    return;
  }

  // A new connection was successfully established.
  // Get a ConnectionHandlerTask to pass the new connection off to.
  auto handler_id = NextDispatchHandlerOffset();
  auto handler = handlers_[handler_id];
  NETWORK_LOG_TRACE("Dispatching connection to worker {}.", handler_id);

  // Notify the chosen ConnectionHandlerTask that it received a new connection.
  handler->Notify(new_conn_fd, provider->Get());
}

void ConnectionDispatcherTask::RunTask() {
  // Create a pool of num_handlers_ many ConnectionHandlerTask instances.
  // The handler tasks are created using the same DedicatedThreadOwner as this ConnectionDispatcherTask.
  for (uint32_t task_id = 0; task_id < num_handlers_; task_id++) {
    auto handler = thread_registry_->RegisterDedicatedThread<ConnectionHandlerTask>(dedicated_thread_owner_, task_id,
                                                                                    connection_handle_factory_);
    handlers_.push_back(handler);
  }
  // After all the connection handlers are ready, the main connection dispatch event loop is run.
  EventLoop();
}

void ConnectionDispatcherTask::Terminate() {
  // End the main connection dispatch event loop.
  ExitLoop();
  // Clean up the all the ConnectionHandlerTask instances.
  for (const auto &handler_task : handlers_) {
    const bool is_task_stopped UNUSED_ATTRIBUTE = thread_registry_->StopTask(
        dedicated_thread_owner_, handler_task.CastManagedPointerTo<common::DedicatedThreadTask>());
    NOISEPAGE_ASSERT(is_task_stopped, "Failed to stop ConnectionHandlerTask.");
  }
}

uint64_t ConnectionDispatcherTask::NextDispatchHandlerOffset() {
  // Get the handler that the next connection should be dispatched to.
  // This is round-robin.
  // TODO(WAN): as inherited from Tianyu, we can be smarter about scheduling dispatch.
  uint64_t handler_id = next_handler_;
  next_handler_ = (next_handler_ + 1) % handlers_.size();
  return handler_id;
}

}  // namespace noisepage::network
