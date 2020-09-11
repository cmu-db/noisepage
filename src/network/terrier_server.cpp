#include "network/terrier_server.h"

#include <arpa/inet.h>
#include <event2/thread.h>

#include <csignal>

#include "common/dedicated_thread_registry.h"
#include "common/settings.h"
#include "common/utility.h"
#include "loggers/network_logger.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

TerrierServer::TerrierServer(common::ManagedPointer<ProtocolInterpreterProvider> protocol_provider,
                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                             common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                             const uint16_t port, const uint16_t connection_thread_count)
    : DedicatedThreadOwner(thread_registry),
      running_(false),
      port_(port),
      max_connections_(connection_thread_count),
      connection_handle_factory_(connection_handle_factory),
      provider_(protocol_provider) {
  // If a client disconnects, the server receives a broken pipe signal SIGPIPE.
  // SIGPIPE by default will kill the server process, which is a bad idea.
  // Instead, the server ignores SIGPIPE.
  // This causes EPIPE to be returned when the server next tries to write to the closed client.
  // This is handled elsewhere, resulting in termination (TODO of?).
  // TODO(WAN): If the client disconnects, shouldn't we just kill off their connection immediately
  //  instead of waiting to try to write to them for the EPIPE?
  signal(SIGPIPE, SIG_IGN);
}

void TerrierServer::RunServer() {
  // Initialize thread support for libevent as libevent will be invoked from multiple ConnectionHandlerTask threads.
  evthread_use_pthreads();

  // Create an internet socket address descriptor.
  struct sockaddr_in sin;
  std::memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port_);

  // Create a new socket.
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to open socket.", strerror(errno)));
  }

  // Enable SO_REUSEADDR.
  // The server opens sockets at a specific (IP address, TCP port).
  // If the server dies or is restarted, there may still be active packets in flight to/from the
  // old dead server, which the new server would receive and be confused by. To prevent this,
  // the TCP port is typically held hostage for twice as long as the maximum packet-in-flight
  // time (2 * /proc/sys/net/ipv4/tcp_fin_timeout seconds). This means that when a new server
  // comes along and tries to rebind to the same (IP address, TCP port), the socket binding
  // will fail. Enabling SO_REUSEADDR opts out of this protection.
  int reuse = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  // Bind the socket.
  int retval = bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sin), sizeof(sin));
  if (retval < 0) {
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to bind socket: {}", strerror(errno)));
  }

  // Prepare to listen to the socket, allowing at most CONNECTION_BACKLOG connection requests
  // to be queued. Any additional requests are dropped.
  retval = listen(listen_fd_, common::Settings::CONNECTION_BACKLOG);
  if (retval < 0) {
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to create listen socket: {}", strerror(errno)));
  }

  // Register the ConnectionDispatcherTask. This allows client connections.
  dispatcher_task_ = thread_registry_->RegisterDedicatedThread<ConnectionDispatcherTask>(
      this, max_connections_, listen_fd_, this, common::ManagedPointer(provider_.Get()), connection_handle_factory_,
      thread_registry_);

  NETWORK_LOG_INFO("Listening on port {0} [PID={1}]", port_, ::getpid());

  // Set the running_ flag for any waiting threads.
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_ = true;
  }
}

void TerrierServer::StopServer() {
  // Stop the dispatcher task and close the socket's file descriptor.
  const bool is_task_stopped UNUSED_ATTRIBUTE =
      thread_registry_->StopTask(this, dispatcher_task_.CastManagedPointerTo<common::DedicatedThreadTask>());
  TERRIER_ASSERT(is_task_stopped, "Failed to stop ConnectionDispatcherTask.");
  TerrierClose(listen_fd_);

  // Clear the running_ flag for any waiting threads.
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_ = false;
  }
  // Wake up any waiting threads.
  running_cv_.notify_all();
}

}  // namespace terrier::network
