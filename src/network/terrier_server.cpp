#include "network/terrier_server.h"

#include <unistd.h>

#include <fstream>
#include <memory>

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
                             const uint16_t port, const uint16_t connection_thread_count)
    : DedicatedThreadOwner(thread_registry),
      running_(false),
      port_(port),
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

void TerrierServer::RunServer() {
  // This line is critical to performance for some reason
  evthread_use_pthreads();

  int conn_backlog = common::Settings::CONNECTION_BACKLOG;

  struct sockaddr_in sin;
  std::memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port_);

  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);

  if (listen_fd_ < 0) {
    NETWORK_LOG_ERROR("Failed to open socket: {}", strerror(errno));
    throw NETWORK_PROCESS_EXCEPTION("Failed to open socket.");
  }

  int reuse = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  int retval = bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sin), sizeof(sin));
  if (retval < 0) {
    NETWORK_LOG_ERROR("Failed to bind socket: {}", strerror(errno));
    throw NETWORK_PROCESS_EXCEPTION("Failed to bind socket.");
  }
  retval = listen(listen_fd_, conn_backlog);
  if (retval < 0) {
    NETWORK_LOG_ERROR("Failed to create listen socket: {}", strerror(errno));
    throw NETWORK_PROCESS_EXCEPTION("Failed to create listen socket.");
  }

  dispatcher_task_ = thread_registry_->RegisterDedicatedThread<ConnectionDispatcherTask>(
      this /* requester */, max_connections_, listen_fd_, this, common::ManagedPointer(provider_.Get()),
      connection_handle_factory_, thread_registry_);

  NETWORK_LOG_INFO("Listening on port {0} [PID={1}]", port_, ::getpid());

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
  TerrierClose(listen_fd_);
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
