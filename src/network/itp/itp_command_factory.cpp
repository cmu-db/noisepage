#include "network/itp/itp_command_factory.h"

#include <memory>
namespace terrier::network {

std::unique_ptr<ITPNetworkCommand> ITPCommandFactory::PacketToCommand(
    const common::ManagedPointer<InputPacket> packet) {
  switch (packet->msg_type_) {
    case NetworkMessageType::ITP_REPLICATION_COMMAND:
      return MAKE_ITP_COMMAND(ReplicationCommand);
    case NetworkMessageType::ITP_STOP_REPLICATION_COMMAND:
      return MAKE_ITP_COMMAND(StopReplicationCommand);
    default:
      throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
  }
}

}  // namespace terrier::network
