#pragma once
#include <memory>

#include "common/managed_pointer.h"
#include "network/postgres/postgres_network_commands.h"

namespace terrier {
namespace network {
struct InputPacket;
}  // namespace network
}  // namespace terrier

#define MAKE_POSTGRES_COMMAND(type) \
  std::unique_ptr<PostgresNetworkCommand>(reinterpret_cast<PostgresNetworkCommand *>(new type(packet)))

namespace terrier::network {

/**
 * PostgresCommandFactory constructs PostgresNetworkCommands that parses input packets to API calls
 * into traffic cop
 */
class PostgresCommandFactory {
 public:
  /**
   * Convert a Postgres packet to command.
   * @param packet the Postgres input packet
   * @return a raw pointer to the converted command
   */
  virtual std::unique_ptr<PostgresNetworkCommand> PacketToCommand(common::ManagedPointer<InputPacket> packet);

  virtual ~PostgresCommandFactory() = default;
};

}  // namespace terrier::network
