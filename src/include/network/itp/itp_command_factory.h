#pragma once
#include <memory>
#include "network/itp/itp_network_commands.h"

#define MAKE_ITP_COMMAND(type) std::static_pointer_cast<ITPNetworkCommand, type>(std::make_shared<type>(packet))

namespace terrier::network {

/**
 * ITPCommandFactory constructs ITPNetworkCommands that parses input packets to API calls
 * into traffic cop
 */
class ITPCommandFactory {
 public:
  /**
   * Convert an ITP packet to command.
   * @param packet the Postgres input packet
   * @return a shared_ptr to the converted command
   */
  virtual std::shared_ptr<ITPNetworkCommand> PacketToCommand(InputPacket *packet);

  virtual ~ITPCommandFactory() = default;
};

}  // namespace terrier::network
