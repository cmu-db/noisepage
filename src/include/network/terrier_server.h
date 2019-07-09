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
#include "common/exception.h"
#include "common/notifiable_task.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"
#include "network/network_types.h"

namespace terrier::network {

/**
 * TerrierServer is the entry point of the network layer
 */
class TerrierServer : public common::DedicatedThreadOwner {
 public:
  /**
   * @brief Constructs a new TerrierServer instance.
   */
  explicit TerrierServer(common::ManagedPointer<ProtocolInterpreter::Provider> protocol_provider,
                         common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                         common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry);

  ~TerrierServer() override = default;

  /**
   * @brief Configure the server to spin up all its threads and start listening
   * on the configured port.
   */
  void RunServer();

  /**
   * Break from the server loop and exit all network handling threads.
   */
  void StopServer();

  /**
   * Set listen port to a new port
   * @param new_port
   */
  void SetPort(uint16_t new_port);

  /**
   * @return true if the server is still running, false otherwise. Use as a predicate if you're waiting on the RunningCV
   * condition variable
   */
  bool Running() const { return running_; }

  /**
   * @return the mutex for waiting on RunningCV
   */
  std::mutex &RunningMutex() { return running_mutex_; }

  /**
   * @return condition variable for a thread to wait on while the server is running. Currently only useful in DBMain
   */
  std::condition_variable &RunningCV() { return running_cv_; }

 private:
  // TODO(Matt): somewhere there's probably a stronger assertion to be made about the state of the server and if
  // threads can be safely taken away, but I don't understand the networking stuff well enough to say for sure what
  // that assertion is
  bool OnThreadRemoval(common::ManagedPointer<common::DedicatedThreadTask> task) override { return true; }

  std::mutex running_mutex_;
  bool running_;
  std::condition_variable running_cv_;

  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint16_t port_;                   // port number
  int listen_fd_ = -1;              // server socket fd that TerrierServer is listening on
  const uint32_t max_connections_;  // maximum number of connections

  common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory_;
  common::ManagedPointer<ProtocolInterpreter::Provider> provider_;
  common::ManagedPointer<ConnectionDispatcherTask> dispatcher_task_;
};
}  // namespace terrier::network
