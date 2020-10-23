#include "network/connection_handle_factory.h"

#include "network/connection_handle.h"
#include "network/protocol_interpreter.h"

namespace noisepage::network {
ConnectionHandle &ConnectionHandleFactory::NewConnectionHandle(int conn_fd,
                                                               std::unique_ptr<ProtocolInterpreter> interpreter,
                                                               common::ManagedPointer<ConnectionHandlerTask> task) {
  // Check if a mapping for the file descriptor already exists.
  std::unordered_map<int, ConnectionHandle>::iterator it;
  {
    // Latch to prevent TOCTOU in between find() and try_emplace().
    common::SpinLatch::ScopedSpinLatch guard(&reusable_handles_latch_);

    it = reusable_handles_.find(conn_fd);
    // If no mapping exists for the file descriptor, a new mapping is created.
    if (it == reusable_handles_.end()) {
      auto ret = reusable_handles_.try_emplace(conn_fd, conn_fd, task, traffic_cop_, std::move(interpreter));
      NOISEPAGE_ASSERT(ret.second, "TOCTOU bug in reusable_handles_.");
      return ret.first->second;
    }
  }

  // An existing mapping for the file descriptor was found.
  // It is assumed that the old mapping is no longer in use, so it is reused.
  // TODO(WAN): How do we check that the mapping is actually no longer in use?
  auto &reused_handle = it->second;
  reused_handle.ResetForReuse(connection_id_t(conn_fd), task, std::move(interpreter));
  return reused_handle;
}
}  // namespace noisepage::network
