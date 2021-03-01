#include "network/noisepage_server.h"

#include <event2/thread.h>
#include <sys/un.h>

#include <csignal>

#include "common/dedicated_thread_registry.h"
#include "common/settings.h"
#include "common/utility.h"
#include "loggers/network_logger.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"

namespace noisepage::network {

TerrierServer::TerrierServer(common::ManagedPointer<ProtocolInterpreterProvider> protocol_provider,
                             common::ManagedPointer<ConnectionHandleFactory> connection_handle_factory,
                             common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                             const uint16_t port, const uint16_t connection_thread_count, std::string socket_directory)
    : DedicatedThreadOwner(thread_registry),
      running_(false),
      port_(port),
      socket_directory_(std::move(socket_directory)),
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

template <TerrierServer::SocketType type>
void TerrierServer::RegisterSocket() {
  static_assert(type == NETWORKED_SOCKET || type == UNIX_DOMAIN_SOCKET, "There should only be two socket types.");

  constexpr auto conn_backlog = common::Settings::CONNECTION_BACKLOG;
  constexpr auto is_networked_socket = type == NETWORKED_SOCKET;
  constexpr auto socket_description = std::string_view(is_networked_socket ? "networked" : "Unix domain");

  auto &socket_fd = is_networked_socket ? network_socket_fd_ : unix_domain_socket_fd_;

  // Get the appropriate sockaddr for the given SocketType. Abuse a lambda and auto to specialize the type.
  auto socket_addr = ([&] {
    if constexpr (is_networked_socket) {  // NOLINT
      // Create an internet socket address descriptor.
      struct sockaddr_in sin = {0};

      sin.sin_family = AF_INET;
      sin.sin_addr.s_addr = INADDR_ANY;
      sin.sin_port = htons(port_);

      return sin;
    } else {  // NOLINT
      // Build the socket path name.
      const std::string socket_path = fmt::format(UNIX_DOMAIN_SOCKET_FORMAT_STRING, socket_directory_, port_);
      struct sockaddr_un sun = {0};

      // Validate the path name, which must be at most the Unix socket path length.
      if (socket_path.length() > sizeof(sun.sun_path)) {
        throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to name {} socket (must have length <= {} characters).",
                                                    socket_description, sizeof(sun.sun_path)));
      }

      sun.sun_family = AF_UNIX;
      socket_path.copy(sun.sun_path, sizeof(sun.sun_path));

      return sun;
    }
  })();

  // Create a new socket.
  socket_fd = socket(is_networked_socket ? AF_INET : AF_UNIX, SOCK_STREAM, 0);

  // Check if the socket was successfully created.
  if (socket_fd < 0) {
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to open {} socket: {}", socket_description, strerror(errno)));
  }

  // Enable SO_REUSEADDR for networked sockets.
  // The server opens sockets at a specific (IP address, TCP port).
  // If the server dies or is restarted, there may still be active packets in flight to/from the
  // old dead server, which the new server would receive and be confused by. To prevent this,
  // the TCP port is typically held hostage for twice as long as the maximum packet-in-flight
  // time (2 * /proc/sys/net/ipv4/tcp_fin_timeout seconds). This means that when a new server
  // comes along and tries to rebind to the same (IP address, TCP port), the socket binding
  // will fail. Enabling SO_REUSEADDR opts out of this protection.
  if constexpr (is_networked_socket) {  // NOLINT
    int reuse = 1;
    setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  }

  // Bind the socket.
  int status = bind(socket_fd, reinterpret_cast<struct sockaddr *>(&socket_addr), sizeof(socket_addr));
  if (status < 0) {
    NETWORK_LOG_ERROR("Failed to bind {} socket: {}", socket_description, strerror(errno), errno);

    // We can recover from exactly one type of error here, contingent on this being a Unix domain socket.
    if constexpr (!is_networked_socket) {  // NOLINT
      auto recovered = false;

      if (errno == EADDRINUSE) {
        // I find this disgusting, but it's the approach favored by a bunch of software that uses Unix domain sockets.
        // BSD syslogd, for example, does this in *every* case--error handling or not--and I'm not one to question it.
        recovered = !std::remove(fmt::format(UNIX_DOMAIN_SOCKET_FORMAT_STRING, socket_directory_, port_).c_str()) &&
                    bind(socket_fd, reinterpret_cast<struct sockaddr *>(&socket_addr), sizeof(socket_addr)) >= 0;
      }

      if (recovered) {
        NETWORK_LOG_INFO("Recovered! Managed to bind {} socket by purging a pre-existing bind.", socket_description);
      } else {
        throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to bind and recover {} socket.", socket_description));
      }
    } else {  // NOLINT
      throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to bind {} socket.", socket_description));
    }
  }

  // Listen on the socket, allowing at most CONNECTION_BACKLOG connection requests to be queued.
  // Any additional requests are dropped.
  status = listen(socket_fd, conn_backlog);
  if (status < 0) {
    throw NETWORK_PROCESS_EXCEPTION(
        fmt::format("Failed to listen on {} socket: {}", socket_description, strerror(errno)));
  }

  NETWORK_LOG_INFO("Listening on {} socket with port {} [PID={}]", socket_description, port_, ::getpid());
}

void TerrierServer::RunServer() {
  // Initialize thread support for libevent as libevent will be invoked from multiple ConnectionHandlerTask threads.
  evthread_use_pthreads();

  // Register the network socket.
  RegisterSocket<NETWORKED_SOCKET>();

  // Register the Unix domain socket.
  RegisterSocket<UNIX_DOMAIN_SOCKET>();

  // Register the ConnectionDispatcherTask. This handles connections to the sockets created above.
  dispatcher_task_ = thread_registry_->RegisterDedicatedThread<ConnectionDispatcherTask>(
      this, max_connections_, this, common::ManagedPointer(provider_.Get()), connection_handle_factory_,
      thread_registry_, std::initializer_list<int>({unix_domain_socket_fd_, network_socket_fd_}));

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
  NOISEPAGE_ASSERT(is_task_stopped, "Failed to stop ConnectionDispatcherTask.");

  // Close the network socket
  TerrierClose(network_socket_fd_);

  // Close the Unix domain socket if it exists
  if (unix_domain_socket_fd_ >= 0) {
    std::remove(fmt::format(UNIX_DOMAIN_SOCKET_FORMAT_STRING, socket_directory_, port_).c_str());
  }

  NETWORK_LOG_INFO("Server Closed");

  // Clear the running_ flag for any waiting threads.
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_ = false;
  }
  // Wake up any waiting threads.
  running_cv_.notify_all();
}

}  // namespace noisepage::network
