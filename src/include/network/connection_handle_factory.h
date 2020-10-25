#pragma once

#include <memory>
#include <unordered_map>

#include "network/connection_handle.h"

namespace noisepage::trafficcop {
class TrafficCop;
}  // namespace noisepage::trafficcop

namespace noisepage::network {

class ConnectionHandlerTask;
class ProtocolInterpreter;

/**
 * @brief ConnectionHandleFactory constructs and reuses ConnectionHandle objects.
 *
 * Reasons for reuse:
 * - ConnectionHandle wraps read and write buffers which are expensive to reallocate. Therefore reuse is desirable.
 * - Moreover, as noted by Tianyu, from a lifetime perspective losing track of a ConnectionHandle leaves the
 *   lost ConnectionHandle completely managed by libevent. libevent does not clean up raw pointers. This leaks memory.
 *
 * @warning As noted in NewConnectionHandle, the file descriptors provided to this factory should no longer be in use.
 */
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
   *
   * @warning If the file descriptor provided is currently in use, the old ConnectionHandle that was associated
   *          with that file descriptor will be completely stomped on. There are no asserts that check this.
   *
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
}  // namespace noisepage::network
