#pragma once
#include <utility>
#include "network/postgres_network_commands.h"

namespace terrier::network
{

class CommandFactory {
 public:
  virtual std::shared_ptr<PostgresNetworkCommand> PacketToCommand(PostgresInputPacket *packet);
};

}