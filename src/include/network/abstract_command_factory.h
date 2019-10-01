#pragma once
#include <memory>
#include "network/abstract_network_commands.h"

namespace terrier::network {

/**
 * A command factory takes in input packets and converts them to commands usable by the protocol interpreters
 */
class AbstractCommandFactory {
 public:
  /**
   * Convert a packet to a command.
   * @param packet the input packet
   * @return a shared_ptr to the converted command
   */
  virtual std::shared_ptr<AbstractNetworkCommand> PacketToCommand(InputPacket *packet) = 0;

  virtual ~AbstractNetworkCommand() = default;
};

}  // namespace terrier::network