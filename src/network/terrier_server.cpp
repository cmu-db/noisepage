#include <network/connection_handle_factory.h>
#include <network/network_defs.h>
#include <fstream>
#include <memory>
#include "common/utility.h"
#include "event2/thread.h"

#include "common/dedicated_thread_registry.h"
#include "network/terrier_server.h"

#include "terrier_config.h"  // NOLINT

namespace terrier::network {

TerrierServer::TerrierServer() {
  port_ = 2888;
  // settings::SettingsManager::GetInt(settings::SettingId::port);
  max_connections_ = 250;
  // settings::SettingsManager::GetInt(settings::SettingId::max_connections);

  // For logging purposes
  //  event_enable_debug_mode();

  //  event_set_log_callback(LogCallback);

  // Commented because it's not in the libevent version we're using
  // When we upgrade this should be uncommented
  //  event_enable_debug_logging(EVENT_DBG_ALL);

  // Ignore the broken pipe signal
  // We don't want to exit on write when the client disconnects
  signal(SIGPIPE, SIG_IGN);
}

TerrierServer &TerrierServer::SetupServer() {
  // This line is critical to performance for some reason
  evthread_use_pthreads();
  /*if (settings::SettingsManager::GetString(
          settings::SettingId::socket_family) != "AF_INET")
    throw ConnectionException("Unsupported socket family");*/

  int conn_backlog = 12;

  struct sockaddr_in sin;
  TERRIER_MEMSET(&sin, 0, sizeof(sin));
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

  LOG_INFO("Listening on port {0}", port_);
  return *this;
}

void TerrierServer::ServerLoop() {
  /*if (settings::SettingsManager::GetBool(settings::SettingId::rpc_enabled)) {
    int rpc_port =
        settings::SettingsManager::GetInt(settings::SettingId::rpc_port);
    std::string address = "127.0.0.1:" + std::to_string(rpc_port);
    auto rpc_task = std::make_shared<PelotonRpcHandlerTask>(address.c_str());
    DedicatedThreadRegistry::GetInstance()
        .RegisterDedicatedThread<PelotonRpcHandlerTask>(this, rpc_task);
  }*/
  dispatcher_task_->EventLoop();

  terrier_close(listen_fd_);

  ConnectionHandleFactory::GetInstance().TearDown();
  LOG_INFO("Server Closed");
}

void TerrierServer::Close() {
  LOG_INFO("Begin to stop server");
  dispatcher_task_->ExitLoop();
}

/**
 * Change port to new_port
 */
void TerrierServer::SetPort(uint16_t new_port) { port_ = new_port; }

}  // namespace terrier::network
