#pragma once

#include <memory>
#include <vector>

#include "common/dedicated_thread_registry.h"
#include "common/notifiable_task.h"

#include "loggers/network_logger.h"

#include "network/connection_handler_task.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * @brief A ConnectionDispatcherTask on the main server thread and dispatches
 * incoming connections to handler threads.
 *
 * On construction, the dispatcher also spawns a number of handlers running on
 * their own threads. The dispatcher is
 * then responsible for maintain, and when shutting down, shutting down the
 * spawned handlers also.
 */
class ConnectionDispatcherTask : public common::NotifiableTask {
 public:
  /**
   * Creates a new ConnectionDispatcherTask, spawning the specified number of
   * handlers, each running on their own threads.
   *
   * @param num_handlers The number of handler tasks to spawn.
   * @param listen_fd The server socket fd to listen on.
   * @param dedicatedThreadOwner The DedicatedThreadOwner associated with this task
   */
  ConnectionDispatcherTask(int num_handlers, int listen_fd, DedicatedThreadOwner *dedicatedThreadOwner);

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
   * Breaks the dispatcher and managed handlers from their event loops.
   */
  void ExitLoop() override;

 private:
  std::vector<std::shared_ptr<ConnectionHandlerTask>> handlers_;
  // TODO(TianyuLi): have a smarter dispatch scheduler, we currently use round-robin
  std::atomic<uint64_t> next_handler_;
};

}  // namespace terrier::network
