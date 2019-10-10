#include "network/connection_handle_factory.h"
#include <memory>
#include <utility>
#include "common/macros.h"
#include "network/connection_handle.h"
#include "network/connection_handler_task.h"

namespace terrier::network {
ConnectionHandle &ConnectionHandleFactory::NewConnectionHandle(int conn_fd,
                                                               std::unique_ptr<ProtocolInterpreter> interpreter,
                                                               common::ManagedPointer<ConnectionHandlerTask> handler) {
  auto it = reusable_handles_.find(conn_fd);
  if (it == reusable_handles_.end()) {
    auto ret = reusable_handles_.try_emplace(conn_fd, conn_fd, handler, traffic_cop_, std::move(interpreter));
    TERRIER_ASSERT(ret.second, "ret.second false");
    return ret.first->second;
  }

  auto &reused_handle = it->second;
  reused_handle.conn_handler_ = handler;
  reused_handle.network_event_ = nullptr;
  reused_handle.workpool_event_ = nullptr;
  reused_handle.io_wrapper_->Restart();
  reused_handle.protocol_interpreter_ = std::move(interpreter);
  reused_handle.state_machine_ = ConnectionHandle::StateMachine();
  reused_handle.context_.Reset();
  TERRIER_ASSERT(reused_handle.network_event_ == nullptr, "network_event_ != nullptr");
  TERRIER_ASSERT(reused_handle.workpool_event_ == nullptr, "network_event_ != nullptr");
  return reused_handle;
}
}  // namespace terrier::network
