#include "messenger/connection_destination.h"

#include "spdlog/fmt/fmt.h"

namespace noisepage::messenger {

ConnectionDestination ConnectionDestination::MakeTCP(std::string target_name, std::string_view hostname, int port) {
  return ConnectionDestination(std::move(target_name), fmt::format("tcp://{}:{}", hostname, port));
}

ConnectionDestination ConnectionDestination::MakeIPC(std::string target_name, std::string_view pathname) {
  return ConnectionDestination(std::move(target_name), fmt::format("ipc://{}", pathname));
}

ConnectionDestination ConnectionDestination::MakeInProc(std::string target_name, std::string_view endpoint) {
  return ConnectionDestination(std::move(target_name), fmt::format("inproc://{}", endpoint));
}

}  // namespace noisepage::messenger
