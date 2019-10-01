#pragma once
#include <memory>
#include "network/abstract_command_factory.h"
#include "network/postgres/postgres_network_commands.h"

#define MAKE_POSTGRES_COMMAND(type) std::static_pointer_cast<PostgresNetworkCommand, type>(std::make_shared<type>(packet))

namespace terrier::network {

/**
 * PostgresCommandFactory constructs PostgresNetworkCommands that parses input packets to API calls
 * into traffic cop
 */
class PostgresCommandFactory : public AbstractCommandFactory {
 public:
  /**
   * Convert a Postgres packet to command.
   * @param packet the Postgres input packet
   * @return a shared_ptr to the converted command
   */
  std::shared_ptr<PostgresNetworkCommand> PacketToCommand(InputPacket *packet);

  ~PostgresCommandFactory() = default;
};

}  // namespace terrier::network
