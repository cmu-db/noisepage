#pragma once

#include <memory>
#include <vector>

#include "common/dedicated_thread_registry.h"
#include "common/managed_pointer.h"
#include "common/notifiable_task.h"
#include "loggers/network_logger.h"
#include "network/connection_handle_factory.h"
#include "network/connection_handler_task.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * @brief A ConnectionDispatcherTask on the main server thread and dispatches
 * incoming connections to handler threads.
 *
 * On RunTask(), the dispatcher registers a number of handlers with the dedicated thread registry. The registered
 * handlers will shut down during Terminate() of this task. ConnectionDispatcherTask is almost a pseudo-owner of the
 * ConnectionHandlerTask objects, but since we can't be both a DedicatedThreadTask/NotifiableTask and
 * DedicatedThreadOwner, we pass the original DedicatedThreadOwner (TerrierServer) value through to the
 * ConnectionHandlerTasks. TerrierServer ends up the DedicatedThreadOwner of both ConnectionDispatcherTask and
 * ConnectionHandlerTasks for its instance.
 *
 */
class ConnectionDispatcherTask : public common::NotifiableTask {
 public:
  /**
   * Creates a new ConnectionDispatcherTask
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
                           common::ManagedPointer<ProtocolInterpreter::Provider> interpreter_provider,
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
   * @param flags Unused. This is here to conform to libevent callback function
   * signature.
   */
  void DispatchConnection(int fd, int16_t flags);

  /**
   * Creates all of the ConnectionHandlerTasks (num_handlers of them) and then sits in its event loop until stopped.
   */
  void RunTask() override;

  /**
   * Exits its event loop and then cleans up all of the ConnectionHandlerTasks before returning.
   */
  void Terminate() override;

 private:
  const uint32_t num_handlers_;
  common::DedicatedThreadOwner *const dedicated_thread_owner_;
  const common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory_;
  const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry_;
  const common::ManagedPointer<ProtocolInterpreter::Provider> interpreter_provider_;
  std::vector<common::ManagedPointer<ConnectionHandlerTask>> handlers_;
  // TODO(TianyuLi): have a smarter dispatch scheduler, we currently use round-robin
  std::atomic<uint64_t> next_handler_;
};

}  // namespace terrier::network
