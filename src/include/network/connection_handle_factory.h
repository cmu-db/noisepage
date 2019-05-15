#pragma once

#include <unordered_map>

#include "network/connection_handle.h"
#include "network/terrier_server.h"
#include "traffic_cop/traffic_cop.h"

namespace terrier::network {

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
   * @return The singleton instance of a ConnectionHandleFactory
   */
  static ConnectionHandleFactory &GetInstance() {
    static ConnectionHandleFactory factory;
    return factory;
  }

  /**
   * @brief Creates or re-purpose a NetworkIoWrapper object for new use.
   * The returned value always uses Posix I/O methods unless explicitly
   * converted.
   * @see NetworkIoWrapper for details
   * @param conn_fd Client connection fd
   * @param task The connection handler task to assign to returned ConnectionHandle object
   * @return A new ConnectionHandle object
   */
  ConnectionHandle &NewConnectionHandle(int conn_fd, ConnectionHandlerTask *task);

  /**
   * Teardown for connection handle factory to clean up anything in reusable_handles_
   */
  void TearDown() {
    DedicatedThreadRegistry::GetInstance().TearDown();
    reusable_handles_.clear();
  }

  /**
   * Tell the factory the dependencies of each connection handle.
   * The current dependencies are: Traffic Cop Pointer, Command Factory Pointer.
   * Then the factory will put them to every connection handle.
   * @param t_cop the pointer to the traffic cop.
   * @param command_factory the pointer to the factory
   */
  void SetConnectionDependencies(TrafficCop* t_cop, CommandFactory *command_factory)
  {
    traffic_cop_ = t_cop;
    command_factory_ = command_factory;
  }

 private:
  std::unordered_map<int, ConnectionHandle> reusable_handles_;
  TrafficCop *traffic_cop_;
  CommandFactory *command_factory_;
};
}  // namespace terrier::network
