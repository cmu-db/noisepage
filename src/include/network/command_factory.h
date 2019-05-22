#pragma once
#include <memory>
#include "network/postgres/postgres_network_commands.h"

namespace terrier::network {

class CommandFactory {
 public:
  virtual std::shared_ptr<PostgresNetworkCommand> PostgresPacketToCommand(PostgresInputPacket *packet);
};

}  // namespace terrier::network
