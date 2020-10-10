#include "messenger/connection_destination.h"

#include "spdlog/fmt/fmt.h"

namespace terrier::messenger {

ConnectionDestination ConnectionDestination::MakeTCP(std::string_view hostname, int port) {
  return ConnectionDestination(fmt::format("tcp://{}:{}", hostname, port));
}

ConnectionDestination ConnectionDestination::MakeIPC(std::string_view pathname) {
  return ConnectionDestination(fmt::format("ipc://{}", pathname));
}

ConnectionDestination ConnectionDestination::MakeInProc(std::string_view endpoint) {
  return ConnectionDestination(fmt::format("inproc://{}", endpoint));
}

}  // namespace terrier::messenger
