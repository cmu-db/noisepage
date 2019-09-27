#pragma once
#include "network/abstract_network_command.h"
// Now we can substitue this with a util class for itp
#include "network/postgres/postgres_protocol_utils.h"

#define DEFINE_ITP_COMMAND(name, flush)                                                                           \
  class name : public ITPNetworkCommand {                                                                \
   public:                                                                                                    \
    explicit name(InputPacket *in) : ITPNetworkCommand(in, flush) {}                                     \
    Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,                                  \
                    common::ManagedPointer<PostgresPacketWriter> out,                                         \
                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,                                     \
                    common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) override; \
  }

namespace terrier::network {

class ITPNetworkCommand : AbstractNetworkCommand {
 public:
  /**
   * Executes the command
   * @param interpreter The protocol interpreter that called this
   * @param out The Writer on which to construct output packets for the client
   * @param t_cop The traffic cop pointer
   * @param connection The ConnectionContext which contains connection information
   * @param callback The callback function to trigger after
   * @return The next transition for the client's state machine
   */
  virtual Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                          common::ManagedPointer<PostgresPacketWriter> out,
                          common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                          common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) = 0;
 protected:
  ITPNetworkCommand(InputPacket *in, bool flush) : AbstractNetworkCommand(in, flush) {}
};

// No commands for now

}  // namespace terrier::network

