#include "network/terrier_server.h"

#include <sys/un.h>
#include <unistd.h>

#include <fstream>
#include <memory>
#include <utility>

#include "common/dedicated_thread_registry.h"
#include "common/settings.h"
#include "common/utility.h"
#include "event2/thread.h"
#include "loggers/network_logger.h"
#include "network/connection_handle_factory.h"
#include "network/network_defs.h"

namespace terrier::network {

TerrierServer::TerrierServer(common::ManagedPointer<ProtocolInterpreter::Provider> protocol_provider,
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
  // For logging purposes
  //  event_enable_debug_mode();

  //  event_set_log_callback(LogCallback);

  // Commented because it's not in the libevent version we're using
  // When we upgrade this should be uncommented
  //  event_enable_debug_logging(EVENT_DBG_ALL);

  // Ignore the broken pipe signal, return EPIPE on pipe write failures.
  // We don't want to exit on write when the client disconnects
  signal(SIGPIPE, SIG_IGN);
}

template <TerrierServer::SocketType type>
void TerrierServer::RegisterSocket() {
  static_assert(type == NETWORKED_SOCKET || type == UNIX_DOMAIN_SOCKET, "There should only be two socket types.");

  constexpr auto conn_backlog = common::Settings::CONNECTION_BACKLOG;
  constexpr auto is_networked_socket = type == NETWORKED_SOCKET;
  constexpr auto socket_description = std::string_view(is_networked_socket ? "networked" : "Unix domain");

  auto &socket_fd = is_networked_socket ? network_socket_fd_ : unix_domain_socket_fd_;

  // Gets the appropriate sockaddr for the given SocketType. Abuse a lambda and auto to specialize the type.
  auto socket_addr = ([&] {
    if constexpr (is_networked_socket) {  // NOLINT
      struct sockaddr_in sin = {0};

      sin.sin_family = AF_INET;
      sin.sin_addr.s_addr = INADDR_ANY;
      sin.sin_port = htons(port_);

      return sin;
    } else {  // NOLINT
      // Builds the socket path name
      const std::string socket_path = fmt::format(UNIX_DOMAIN_SOCKET_FORMAT_STRING, socket_directory_, port_);
      struct sockaddr_un sun = {0};

      // Validate pathname
      if (socket_path.length() > sizeof(sun.sun_path) /* Max Unix socket path length */) {
        NETWORK_LOG_ERROR(fmt::format("Domain socket name too long (must be <= {} characters)", sizeof(sun.sun_path)));
        throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to name {} socket.", socket_description));
      }

      sun.sun_family = AF_UNIX;
      socket_path.copy(sun.sun_path, sizeof(sun.sun_path));

      return sun;
    }
  })();

  // Create socket
  socket_fd = socket(is_networked_socket ? AF_INET : AF_UNIX, SOCK_STREAM, 0);

  // Check if socket was successfully created
  if (socket_fd < 0) {
    NETWORK_LOG_ERROR("Failed to open {} socket: {}", socket_description, strerror(errno));
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to open {} socket.", socket_description));
  }

  // For networked sockets, tell the kernel that we would like to reuse local addresses whenever possible.
  if constexpr (is_networked_socket) {  // NOLINT
    int reuse = 1;
    setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  }

  // Bind the socket
  int status = bind(socket_fd, reinterpret_cast<struct sockaddr *>(&socket_addr), sizeof(socket_addr));
  if (status < 0) {
    NETWORK_LOG_ERROR("Failed to bind {} socket: {}", socket_description, strerror(errno), errno);

    // We can recover from exactly one type of error here, contingent on this being a Unix domain socket.
    if constexpr (!is_networked_socket) {  // NOLINT
      auto recovered = false;

      if (errno == EADDRINUSE) {
        // I find this disgusting, but it's the approach favored by a bunch of software that uses Unix domain sockets.
        // BSD syslogd, for example, does this in *every* case--error handling or not--and I'm not one to question it.
        // To elaborate, I strongly dislike the idea of overwriting existing domain sockets. The idea is that in some
        // edge cases (say, the process gets kill -9'd or crashes) the OS will not remove the existing Unix socket, so
        // we have to delete it ourselves. You would think there'd be a better way to handle such cases--and technically
        // there is, with Linux abstract namespace sockets--but it's non-portable and it's incompatible with psql.
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

  // Listen on the socket
  status = listen(socket_fd, conn_backlog);
  if (status < 0) {
    NETWORK_LOG_ERROR("Failed to listen on {} socket: {}", socket_description, strerror(errno));
    throw NETWORK_PROCESS_EXCEPTION(fmt::format("Failed to listen on {} socket.", socket_description));
  }

  NETWORK_LOG_INFO("Listening on {} socket with port {} [PID={}]", socket_description, port_, ::getpid());
}

void TerrierServer::RunServer() {
  // This line is critical to performance for some reason
  evthread_use_pthreads();

  // Register the network socket
  RegisterSocket<NETWORKED_SOCKET>();

  // Register the Unix domain socket
  RegisterSocket<UNIX_DOMAIN_SOCKET>();

  // Create a dispatcher to handle connections to the sockets that have been created.
  dispatcher_task_ = thread_registry_->RegisterDedicatedThread<ConnectionDispatcherTask>(
      this /* requester */, max_connections_, this, common::ManagedPointer(provider_.Get()), connection_handle_factory_,
      thread_registry_, std::initializer_list<int>({unix_domain_socket_fd_, network_socket_fd_}));

  // Set the running_ flag for any waiting threads
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_ = true;
  }
}

void TerrierServer::StopServer() {
  NETWORK_LOG_TRACE("Begin to stop server");
  const bool result UNUSED_ATTRIBUTE =
      thread_registry_->StopTask(this, dispatcher_task_.CastManagedPointerTo<common::DedicatedThreadTask>());
  TERRIER_ASSERT(result, "Failed to stop ConnectionDispatcherTask.");

  // Close the network socket
  TerrierClose(network_socket_fd_);

  // Close the Unix domain socket if it exists
  if (unix_domain_socket_fd_ >= 0) {
    std::remove(fmt::format(UNIX_DOMAIN_SOCKET_FORMAT_STRING, socket_directory_, port_).c_str());
  }

  NETWORK_LOG_INFO("Server Closed");

  // Clear the running_ flag for any waiting threads and wake up them up with the condition variable
  {
    std::lock_guard<std::mutex> lock(running_mutex_);
    running_ = false;
  }
  running_cv_.notify_all();
}

/**
 * Change port to new_port
 */
void TerrierServer::SetPort(uint16_t new_port) { port_ = new_port; }

}  // namespace terrier::network
