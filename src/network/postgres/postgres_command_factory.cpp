#include "network/postgres/postgres_command_factory.h"

#include <memory>
namespace terrier::network {

std::unique_ptr<PostgresNetworkCommand> PostgresCommandFactory::PacketToCommand(
    const common::ManagedPointer<InputPacket> packet) {
  switch (packet->msg_type_) {
    case NetworkMessageType::PG_SIMPLE_QUERY_COMMAND:
      return MAKE_POSTGRES_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PG_PARSE_COMMAND:
      return MAKE_POSTGRES_COMMAND(ParseCommand);
    case NetworkMessageType::PG_BIND_COMMAND:
      return MAKE_POSTGRES_COMMAND(BindCommand);
    case NetworkMessageType::PG_DESCRIBE_COMMAND:
      return MAKE_POSTGRES_COMMAND(DescribeCommand);
    case NetworkMessageType::PG_EXECUTE_COMMAND:
      return MAKE_POSTGRES_COMMAND(ExecuteCommand);
    case NetworkMessageType::PG_SYNC_COMMAND:
      return MAKE_POSTGRES_COMMAND(SyncCommand);
    case NetworkMessageType::PG_CLOSE_COMMAND:
      return MAKE_POSTGRES_COMMAND(CloseCommand);
    case NetworkMessageType::PG_TERMINATE_COMMAND:
      return MAKE_POSTGRES_COMMAND(TerminateCommand);
    default:
      throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
  }
}

}  // namespace terrier::network
