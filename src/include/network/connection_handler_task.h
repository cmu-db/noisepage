#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <unistd.h>

#include "common/exception.h"
#include "common/notifiable_task.h"
#include "loggers/main_logger.h"
#include "network/network_defs.h"

namespace terrier::network {

class ConnectionHandleFactory;

/**
 * A ConnectionHandlerTask is responsible for interacting with a client
 * connection.
 *
 * A client connection, once taken by the dispatch, is sent to a handler.
 * Then all related client events are registered in the handler task.
 * All client interaction happens on the same ConnectionHandlerTask thread for the entire lifetime of the connection.
 */
class ConnectionHandlerTask : public common::NotifiableTask {
 public:
  /**
   * Constructs a new ConnectionHandlerTask instance.
   * @param task_id task_id a unique id assigned to this task.
   * @param connection_handle_factory The pointer to the connection handle factory
   */
  explicit ConnectionHandlerTask(int task_id, ConnectionHandleFactory *connection_handle_factory);

  /**
   * @brief Notifies this ConnectionHandlerTask that a new client connection
   * should be handled at socket fd.
   *
   * This method is meant to be invoked on another thread (probably the
   * dispatcher) to update
   * the necessary data structure so the handler thread is woken up.
   *
   * @param conn_fd the client connection socket fd.
   * @param protocol_type the protocol used for this socket fd
   */
  void Notify(int conn_fd, NetworkProtocolType protocol_type);

  /**
   * @brief Handles a new client assigned to this handler by the dispatcher.
   *
   * This method will create the necessary data structure for the client and
   * register its event base to receive updates with appropriate callbacks
   * when the client writes to the socket.
   *
   * @param new_conn_recv_fd the socket fd of the new connection
   * @param flags unused. For compliance with libevent callback interface.
   */
  void HandleDispatch(int new_conn_recv_fd, int16_t flags);

 private:
  int client_fd_;
  event *notify_event_;
  NetworkProtocolType protocol_type_;

  ConnectionHandleFactory *connection_handle_factory_;
};

}  // namespace terrier::network
