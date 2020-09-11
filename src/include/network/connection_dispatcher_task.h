#pragma once

#include <vector>

#include "common/managed_pointer.h"
#include "common/notifiable_task.h"

namespace terrier::common {
class DedicatedThreadRegistry;
class DedicatedThreadOwner;
}  // namespace terrier::common

namespace terrier::network {

class ConnectionHandleFactory;
class ConnectionHandlerTask;
class ProtocolInterpreterProvider;

/**
 * @brief ConnectionDispatcherTask dispatches incoming connections to a pool of handler threads.
 *
 * Task life-cycle:
 * - RunTask() : This task registers all of its ConnectionHandlerTask instances with the DedicatedThreadRegistry.
 * - Terminate() : This task stops and removes all its ConnectionHandlerTask instances from the DedicatedThreadRegistry.
 *
 * ConnectionDispatcherTask is almost a pseudo-owner of the
 * ConnectionHandlerTask objects, but since we can't be both a DedicatedThreadTask/NotifiableTask and
 * DedicatedThreadOwner, we pass the original DedicatedThreadOwner (TerrierServer) value through to the
 * ConnectionHandlerTasks. TerrierServer ends up the DedicatedThreadOwner of both ConnectionDispatcherTask and
 * ConnectionHandlerTasks for its instance.
 */
class ConnectionDispatcherTask : public common::NotifiableTask {
 public:
  /**
   * Create a new ConnectionDispatcherTask.
   *
   * @param num_handlers The number of handler tasks to spawn.
   * @param listen_fd The server socket fd to listen on.
   * @param dedicated_thread_owner The DedicatedThreadOwner associated with this task
   * @param interpreter_provider provider that constructs protocol interpreters
   * @param connection_handle_factory The connection handle factory pointer to pass down to the handlers
   * @param thread_registry DedicatedThreadRegistry dependency needed because it eventually spawns more threads in
   * RunTask
   */
  ConnectionDispatcherTask(uint32_t num_handlers, int listen_fd, common::DedicatedThreadOwner *dedicated_thread_owner,
                           common::ManagedPointer<ProtocolInterpreterProvider> interpreter_provider,
                           common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                           common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry);

  /**
   * @brief Dispatches the client connection at fd to a handler.
   * Currently, the dispatch uses round-robin, and thread communication is
   * achieved
   * through channels. The dispatch writes a symbol to the fd that the handler
   * is configured
   * to receive updates on.
   *
   * @param fd the socket fd of the client connection being dispatched
   * @param provider The protocol that should be used to handle this request.
   */
  void DispatchConnection(uint32_t fd, common::ManagedPointer<ProtocolInterpreterProvider> provider);

  /**
   * Creates all of the ConnectionHandlerTasks (num_handlers of them) and then sits in its event loop until stopped.
   */
  void RunTask() override;

  /**
   * Exits its event loop and then cleans up all of the ConnectionHandlerTasks before returning.
   */
  void Terminate() override;

 private:
  /** @return The offset in handlers_ of the next handler to dispatch to. This function mutates internal state. */
  uint64_t NextDispatchHandlerOffset();

  /** The number of */
  const uint32_t num_handlers_;
  common::DedicatedThreadOwner *const dedicated_thread_owner_;
  const common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory_;
  const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry_;
  const common::ManagedPointer<ProtocolInterpreterProvider> interpreter_provider_;
  std::vector<common::ManagedPointer<ConnectionHandlerTask>> handlers_;
  std::atomic<uint64_t> next_handler_;
};

}  // namespace terrier::network
