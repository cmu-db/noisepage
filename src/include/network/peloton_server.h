//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_server.h
//
// Identification: src/include/network/peloton_server.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <pthread.h>
#include <sys/file.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/container/lock_free_queue.h"
#include "common/dedicated_thread_owner.h"
#include "common/exception.h"
#include "common/notifiable_task.h"
#include "connection_dispatcher_task.h"
#include "loggers/main_logger.h"
#include "network_types.h"

namespace terrier {
namespace network {

/**
 * PelotonServer is the entry point of the network layer
 */
class TerrierServer : public DedicatedThreadOwner {
 public:
  /**
   * @brief Constructs a new PelotonServer instance.
   *
   * Note that SettingsManager must already be initialized when this constructor
   * is called.
   */
  TerrierServer();

  /**
   * @brief Configure the server to spin up all its threads and start listening
   * on the configured port.
   *
   * This is separated from the main loop primarily for testing purposes, as we
   * need to wait for the server
   * to start listening on the port before the rest of the test. All
   * event-related settings are also performed
   * here. Since libevent reacts to events fired before the event loop as well,
   * all interactions to the server
   * after this function returns is guaranteed to be handled. For non-testing
   * purposes, you can chain the functions,
   * e.g.:
   *
   *   server.SetupServer().ServerLoop();
   *
   * @return self-reference for chaining
   */
  TerrierServer &SetupServer();

  /**
   * @brief In a loop, handles incoming connection and block the current thread
   * until closed.
   *
   * The loop will exit when either Close() is explicitly called or when there
   * are no more events pending or
   * active (we currently register all events to be persistent.)
   */
  void ServerLoop();

  /**
   * Break from the server loop and exit all network handling threads.
   */
  void Close();

  // TODO(tianyu): This is VILE. Fix this when we refactor testing.
  void SetPort(uint16_t new_port);

 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint16_t port_;           // port number
  int listen_fd_ = -1;      // server socket fd that TerrierServer is listening on
  size_t max_connections_;  // maximum number of connections

  // For testing purposes
  std::shared_ptr<ConnectionDispatcherTask> dispatcher_task_;
};
}  // namespace network
}  // namespace terrier
