#pragma once
#include "network/abstract_network_command.h"

#define DEFINE_POSTGRES_COMMAND(name, flush)                                                                  \
  class name : public PostgresNetworkCommand {                                                                \
   public:                                                                                                    \
    explicit name(InputPacket *in) : PostgresNetworkCommand(in, flush) {}                                     \
    Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,                                  \
                    common::ManagedPointer<AbstractPacketWriter> out,                                         \
                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,                                     \
                    common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) override; \
  }

namespace terrier::network {

class PostgresNetworkCommand : public AbstractNetworkCommand {
 protected:
  PostgresNetworkCommand(InputPacket *in, bool flush) : AbstractNetworkCommand(in, flush) {}
};

// Set all to force flush for now
DEFINE_POSTGRES_COMMAND(SimpleQueryCommand, true);
DEFINE_POSTGRES_COMMAND(ParseCommand, true);
DEFINE_POSTGRES_COMMAND(BindCommand, true);
DEFINE_POSTGRES_COMMAND(DescribeCommand, true);
DEFINE_POSTGRES_COMMAND(ExecuteCommand, true);
DEFINE_POSTGRES_COMMAND(SyncCommand, true);
DEFINE_POSTGRES_COMMAND(CloseCommand, true);
DEFINE_POSTGRES_COMMAND(TerminateCommand, true);

DEFINE_POSTGRES_COMMAND(EmptyCommand, true);

}  // namespace terrier::network
