#pragma once

#include <memory>
#include <unordered_map>
#include "common/dedicated_thread_registry.h"
#include "network/connection_handle.h"
#include "network/connection_handler_task.h"
#include "network/postgres/postgres_command_factory.h"
#include "traffic_cop/traffic_cop.h"

namespace terrier::network {

class ConnectionHandlerTask;

/**
 * @brief Factory class for constructing ConnectionHandle objects
 * Each ConnectionHandle is associated with read and write buffers that are
 * expensive to reallocate on the fly. Thus, instead of destroying these wrapper
 * objects when they are out of scope, we save them until we can transfer their
 * buffers to other wrappers.
 */
// TODO(Tianyu): Additionally, it is hard to make sure the ConnectionHandles
// don't leak without this factory since they are essentially managed by
// libevent if nothing in our system holds reference to them, and libevent
// doesn't cleanup raw pointers.
class ConnectionHandleFactory {
 public:
  /**
   * Builds a new connection handle factory.
   * @param tcop The pointer to the traffic cop
   */
  explicit ConnectionHandleFactory(common::ManagedPointer<trafficcop::TrafficCop> tcop) : traffic_cop_(tcop) {}

  /**
   * @brief Creates or re-purpose a NetworkIoWrapper object for new use.
   * The returned value always uses Posix I/O methods unless explicitly
   * converted.
   * @see NetworkIoWrapper for details
   * @param conn_fd Client connection fd
   * @param interpreter The protocol interpreter to use for this connection handle
   * @param handler The connection handler task to assign to returned ConnectionHandle object
   * @return A new ConnectionHandle object
   */
  ConnectionHandle &NewConnectionHandle(int conn_fd, std::unique_ptr<ProtocolInterpreter> interpreter,
                                        common::ManagedPointer<ConnectionHandlerTask> handler);

 private:
  std::unordered_map<int, ConnectionHandle> reusable_handles_;
  common::ManagedPointer<trafficcop::TrafficCop> traffic_cop_;
};
}  // namespace terrier::network
