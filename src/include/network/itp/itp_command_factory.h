#pragma once
#include <memory>
#include "network/itp/itp_network_commands.h"

#define MAKE_ITP_COMMAND(type) \
  std::unique_ptr<ITPNetworkCommand>(static_cast<ITPNetworkCommand *>(std::make_unique<type>(packet).release()))

namespace terrier::network {

/**
 * ITPCommandFactory constructs ITPNetworkCommands that parses input packets to API calls
 * for traffic cop
 */
class ITPCommandFactory {
 public:
  /**
   * Convert an ITP packet to command.
   * @param packet the Postgres input packet
   * @return a unique_ptr to the converted command
   */
  virtual std::unique_ptr<ITPNetworkCommand> PacketToCommand(InputPacket *packet);

  virtual ~ITPCommandFactory() = default;
};

}  // namespace terrier::network
