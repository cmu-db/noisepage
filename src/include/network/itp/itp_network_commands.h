#pragma once
#include "network/abstract_network_command.h"
#include "network/itp/itp_packet_writer.h"

#define DEFINE_ITP_COMMAND(name, flush)                                                                       \
  class name : public ITPNetworkCommand {                                                                     \
   public:                                                                                                    \
    explicit name(InputPacket *in) : ITPNetworkCommand(in, flush) {}                                          \
    Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,                                  \
                    common::ManagedPointer<AbstractPacketWriter> out,                                         \
                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,                                     \
                    common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) override; \
  }

namespace terrier::network {

/**
 * Interface for the execution of the standard ITPNetworkCommands for the ITP protocol
 */
class ITPNetworkCommand : AbstractNetworkCommand {
 protected:
  ITPNetworkCommand(InputPacket *in, bool flush) : AbstractNetworkCommand(in, flush) {}
};

// No commands for now

}  // namespace terrier::network
