#include "network/postgres/postgres_command_factory.h"
#include <memory>
namespace terrier::network {

std::shared_ptr<PostgresNetworkCommand> PostgresCommandFactory::PacketToCommand(InputPacket *packet) {
  switch (packet->msg_type_) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND:
      return MAKE_POSTGRES_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PARSE_COMMAND:
      return MAKE_POSTGRES_COMMAND(ParseCommand);
    case NetworkMessageType::BIND_COMMAND:
      return MAKE_POSTGRES_COMMAND(BindCommand);
    case NetworkMessageType::DESCRIBE_COMMAND:
      return MAKE_POSTGRES_COMMAND(DescribeCommand);
    case NetworkMessageType::EXECUTE_COMMAND:
      return MAKE_POSTGRES_COMMAND(ExecuteCommand);
    case NetworkMessageType::SYNC_COMMAND:
      return MAKE_POSTGRES_COMMAND(SyncCommand);
    case NetworkMessageType::CLOSE_COMMAND:
      return MAKE_POSTGRES_COMMAND(CloseCommand);
    case NetworkMessageType::TERMINATE_COMMAND:
      return MAKE_POSTGRES_COMMAND(TerminateCommand);
    default:
      throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
  }
}

}  // namespace terrier::network
