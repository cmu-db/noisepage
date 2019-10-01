#pragma once
#include <memory>
#include "network/abstract_command_factory.h"
#include "network/itp/itp_network_commands.h"

#define MAKE_ITP_COMMAND(type) std::static_pointer_cast<ITPNetworkCommand, type>(std::make_shared<type>(packet))

namespace terrier::network {

/**
 * ITPCommandFactory constructs ITP commands from parsed input packets.
 */
class ITPCommandFactory : public AbstractCommandFactory{
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
