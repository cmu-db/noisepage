#include "network/connection_handler_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id) : NotifiableTask(task_id) {
  int fds[2];
  if (pipe(fds) != 0) {
    NETWORK_LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  new_conn_send_fd_ = fds[1];
  RegisterEvent(fds[0], EV_READ | EV_PERSIST, METHOD_AS_CALLBACK(ConnectionHandlerTask, HandleDispatch), this);
}

void ConnectionHandlerTask::Notify(int conn_fd) {
  int buf[1];
  buf[0] = conn_fd;
  if (write(new_conn_send_fd_, buf, sizeof(int)) != sizeof(int)) {
    NETWORK_LOG_ERROR("Failed to write to thread notify pipe");
  }
}

void ConnectionHandlerTask::HandleDispatch(int new_conn_recv_fd, int16_t) {  // NOLINT as we don't use the flags arg
  // buffer used to receive messages from the main thread
  char client_fd[sizeof(int)];
  size_t bytes_read = 0;

  // read fully
  while (bytes_read < sizeof(int)) {
    ssize_t result = read(new_conn_recv_fd, client_fd + bytes_read, sizeof(int) - bytes_read);
    if (result < 0) {
      NETWORK_LOG_ERROR("Error when reading from dispatch: errno {}", errno);
    } else {
      bytes_read += static_cast<size_t>(result);
    }
  }
  ConnectionHandleFactory::GetInstance()
      .NewConnectionHandle(*reinterpret_cast<int *>(client_fd), this)
      .RegisterToReceiveEvents();
}

}  // namespace terrier::network
