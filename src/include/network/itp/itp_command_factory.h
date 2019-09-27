#pragma once
#include <memory>
#include "network/itp/itp_network_commands.h"

#define MAKE_COMMAND(type) std::static_pointer_cast<PostgresNetworkCommand, type>(std::make_shared<type>(packet))

namespace terrier::network {

/**
 * ITPCommandFactory constructs ITP commands from parsed input packets.
 */
class ITPCommandFactory {
 public:
  /**
   * Convert an ITP packet to command.
   * @param packet the Postgres input packet
   * @return a shared_ptr to the converted command
   */
  std::shared_ptr<ITPNetworkCommand> PacketToCommand(InputPacket *packet);

  ~ITPCommandFactory() = default;
};

}  // namespace terrier::network
