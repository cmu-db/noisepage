#include <fstream>
#include <memory>
#include "common/settings.h"
#include "common/utility.h"
#include "event2/thread.h"
#include "network/connection_handle_factory.h"
#include "network/network_defs.h"

#include "loggers/network_logger.h"

#include "common/dedicated_thread_registry.h"
#include "network/terrier_server.h"

namespace terrier::network {

TerrierServer::TerrierServer() {
  port_ = common::Settings::SERVER_PORT;
  // settings::SettingsManager::GetInt(settings::SettingId::port);
  max_connections_ = common::Settings::MAX_CONNECTIONS;
  // settings::SettingsManager::GetInt(settings::SettingId::max_connections);

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

TerrierServer &TerrierServer::SetupServer() {
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
    throw NETWORK_PROCESS_EXCEPTION("Failed to create listen socket");
  }

  int reuse = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sin), sizeof(sin));
  listen(listen_fd_, conn_backlog);

  dispatcher_task_ = std::make_shared<ConnectionDispatcherTask>(CONNECTION_THREAD_COUNT, listen_fd_, this);

  NETWORK_LOG_INFO("Listening on port {0}", port_);
  return *this;
}

void TerrierServer::ServerLoop() {
  dispatcher_task_->EventLoop();
  ShutDown();
}

void TerrierServer::ShutDown() {
  terrier_close(listen_fd_);
  ConnectionHandleFactory::GetInstance().TearDown();
  NETWORK_LOG_INFO("Server Closed");
}

void TerrierServer::Close() {
  NETWORK_LOG_TRACE("Begin to stop server");
  dispatcher_task_->ExitLoop();
}

/**
 * Change port to new_port
 */
void TerrierServer::SetPort(uint16_t new_port) { port_ = new_port; }

}  // namespace terrier::network
