#pragma once

#include <memory>
#include <unordered_map>

#include "network/connection_handle.h"

namespace terrier::trafficcop {
class TrafficCop;
}  // namespace terrier::trafficcop

namespace terrier::network {

class ConnectionHandlerTask;
class ProtocolInterpreter;

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
  /** Instantiate a new ConnectionHandleFactory that uses the provided TrafficCop for all the handles created. */
  explicit ConnectionHandleFactory(common::ManagedPointer<trafficcop::TrafficCop> tcop) : traffic_cop_(tcop) {}

  /**
   * @brief Create a new connection handle.
   *
   * @param conn_fd File descriptor for the client connection.
   * @param interpreter The protocol interpreter to use for the new ConnectionHandle.
   * @param task The task to be assigned to the new ConnectionHandle.
   * @return A new ConnectionHandle object.
   */
  ConnectionHandle &NewConnectionHandle(int conn_fd, std::unique_ptr<ProtocolInterpreter> interpreter,
                                        common::ManagedPointer<ConnectionHandlerTask> task);

 private:
  /** This latch protects reusable_handles_. */
  common::SpinLatch reusable_handles_latch_;
  /** Maps from file descriptors -> Connection Handle. Protected by reusable_handles_latch_.*/
  std::unordered_map<int, ConnectionHandle> reusable_handles_;
  common::ManagedPointer<trafficcop::TrafficCop> traffic_cop_;
};
}  // namespace terrier::network
