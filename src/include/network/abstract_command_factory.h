#pragma once
#include <memory>
#include "network/abstract_network_command.h"

#define MAKE_COMMAND(type) std::static_pointer_cast<AbstractNetworkCommand, type>(std::make_shared<type>(packet))

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

  virtual ~AbstractCommandFactory() = default;
};

}  // namespace terrier::network
