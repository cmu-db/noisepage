#include "network/connection_dispatcher_task.h"

#include <common/dedicated_thread_registry.h>

#include <csignal>
#include <memory>

#define MASTER_THREAD_ID (-1)

namespace terrier::network {

ConnectionDispatcherTask::ConnectionDispatcherTask(int num_handlers, int listen_fd,
                                                   DedicatedThreadOwner *dedicatedThreadOwner,
                                                   ConnectionHandleFactory *connection_handle_factory)
    : NotifiableTask(MASTER_THREAD_ID), next_handler_(0) {
  RegisterEvent(listen_fd, EV_READ | EV_PERSIST,
                METHOD_AS_CALLBACK(ConnectionDispatcherTask, DispatchPostgresConnection), this);
  RegisterSignalEvent(SIGHUP, METHOD_AS_CALLBACK(NotifiableTask, ExitLoop), this);

  // create worker threads.
  for (int task_id = 0; task_id < num_handlers; task_id++) {
    // auto handler = new ConnectionHandlerTask(task_id, connection_handle_factory);
    auto handler = DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread<ConnectionHandlerTask>(
        dedicatedThreadOwner, task_id, connection_handle_factory);
    handlers_.push_back(handler);
  }
}

void ConnectionDispatcherTask::DispatchPostgresConnection(int fd, int16_t) {  // NOLINT
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

  handler->Notify(new_conn_fd, NetworkProtocolType::POSTGRES_PSQL);
}
}  // namespace terrier::network
