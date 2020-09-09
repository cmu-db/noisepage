#pragma once

#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/file.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "common/dedicated_thread_owner.h"
#include "common/error/exception.h"
#include "common/notifiable_task.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"
#include "network/network_types.h"

namespace terrier::network {

/** TerrierServer is the entry point to the network layer. */
class TerrierServer : public common::DedicatedThreadOwner {
 public:
  /** @brief Construct a new TerrierServer instance. */
  TerrierServer(common::ManagedPointer<ProtocolInterpreterProvider> protocol_provider,
                common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry, uint16_t port,
                uint16_t connection_thread_count);

  /** @brief Destructor. */
  ~TerrierServer() override = default;

  /** @brief Spin up all of the server threads and start listening on the configured port. */
  void RunServer();

  /** @brief Break from the server loop and exit all network handling threads. */
  void StopServer();

  /** @return True if the server is still running. Use as a predicate when waiting on RunningCV. */
  bool Running() const { return running_; }

  /** @return Mutex for waiting on RunningCV. */
  std::mutex &RunningMutex() { return running_mutex_; }

  /** @return Condvar for a thread to wait on while the server is running. Currently only useful in DBMain. */
  std::condition_variable &RunningCV() { return running_cv_; }

 private:
  // TODO(Matt): somewhere there's probably a stronger assertion to be made about the state of the server and if
  // threads can be safely taken away, but I don't understand the networking stuff well enough to say for sure what
  // that assertion is
  bool OnThreadRemoval(common::ManagedPointer<common::DedicatedThreadTask> task) override { return true; }

  std::mutex running_mutex_;
  bool running_;
  std::condition_variable running_cv_;

  /** The port number of the server. */
  uint16_t port_;
  /** The socket file descriptor that the server is listening on. */
  int listen_fd_ = -1;
  /** The maximum number of connections to the server. */
  const uint32_t max_connections_;

  common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory_;
  common::ManagedPointer<ProtocolInterpreterProvider> provider_;
  common::ManagedPointer<ConnectionDispatcherTask> dispatcher_task_;
};
}  // namespace terrier::network
