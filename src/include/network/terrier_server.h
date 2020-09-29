#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>

#include "common/dedicated_thread_owner.h"

namespace terrier::network {

class ConnectionDispatcherTask;
class ConnectionHandleFactory;
class ProtocolInterpreterProvider;

// The name is based on https://www.postgresql.org/docs/9.3/runtime-config-connection.html
constexpr std::string_view UNIX_DOMAIN_SOCKET_FORMAT_STRING = "{0}/.s.PGSQL.{1}";

/** TerrierServer is the entry point to the network layer. */
class TerrierServer : public common::DedicatedThreadOwner {
 public:
  /** @brief Construct a new TerrierServer instance. */
  TerrierServer(common::ManagedPointer<ProtocolInterpreterProvider> protocol_provider,
                common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry, uint16_t port,
                uint16_t connection_thread_count, std::string socket_directory);

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
  enum SocketType { UNIX_DOMAIN_SOCKET, NETWORKED_SOCKET };

  template <SocketType type>
  void RegisterSocket();

  std::mutex running_mutex_;
  bool running_;
  std::condition_variable running_cv_;

  /** The port number of the server. */
  uint16_t port_;
  /** The networked socket file descriptor that the server is listening on. */
  int network_socket_fd_ = -1;
  /** The unix-based local socket file descriptor that the server may be listening on. */
  int unix_domain_socket_fd_ = -1;
  /** The directory to store the Unix domain socket. */
  const std::string socket_directory_;
  /** The maximum number of connections to the server. */
  const uint32_t max_connections_;

  common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory_;
  common::ManagedPointer<ProtocolInterpreterProvider> provider_;
  common::ManagedPointer<ConnectionDispatcherTask> dispatcher_task_;
};
}  // namespace terrier::network
