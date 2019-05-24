#pragma once
#include <memory>
#include "network/postgres/postgres_network_commands.h"

namespace terrier::network {

/**
 * CommandFactory is where we convert input packet to traffic cop commands.
 */

class CommandFactory {
 public:
  /**
   * Convert a Postgres packet to command.
   * @param packet the Postgres input packet
   * @return a shared_ptr to the converted command
   */
  virtual std::shared_ptr<PostgresNetworkCommand> PostgresPacketToCommand(PostgresInputPacket *packet);
};

}  // namespace terrier::network
