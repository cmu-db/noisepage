#include "network/connection_handler_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id, ConnectionHandleFactory *connection_handle_factory)
    : NotifiableTask(task_id), connection_handle_factory_(connection_handle_factory) {
  notify_event_ =
      RegisterEvent(-1, EV_READ | EV_PERSIST, METHOD_AS_CALLBACK(ConnectionHandlerTask, HandleDispatch), this);
}

void ConnectionHandlerTask::Notify(int conn_fd, NetworkProtocolType protocol_type) {
  client_fd_ = conn_fd;
  protocol_type_ = protocol_type;
  int res = 0;         // Flags, unused attribute in event_active
  int16_t ncalls = 0;  // Unused attribute in event_active
  event_active(notify_event_, res, ncalls);
}

void ConnectionHandlerTask::HandleDispatch(int, int16_t) {  // NOLINT as we don't use the flags arg nor the fd
  connection_handle_factory_->NewConnectionHandle(client_fd_, NetworkProtocolType::POSTGRES_PSQL, this)
      .RegisterToReceiveEvents();
}

}  // namespace terrier::network
