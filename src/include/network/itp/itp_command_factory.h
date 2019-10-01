#pragma once
#include <memory>
#include "network/abstract_command_factory.h"
#include "network/itp/itp_network_commands.h"

namespace terrier::network {

/**
 * ITPCommandFactory constructs ITP commands from parsed input packets.
 */
class ITPCommandFactory : public AbstractCommandFactory {
 public:
  /**
   * Convert an ITP packet to command.
   * @param packet the Postgres input packet
   * @return a shared_ptr to the converted command
   */
  std::shared_ptr<AbstractNetworkCommand> PacketToCommand(InputPacket *packet) override;

  ~ITPCommandFactory() = default;
};

}  // namespace terrier::network
