#include "network/itp/itp_command_factory.h"
#include <memory>
namespace terrier::network {

std::shared_ptr<ITPNetworkCommand> ITPCommandFactory::PacketToCommand(InputPacket *packet) {
  throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
}

}  // namespace terrier::network
