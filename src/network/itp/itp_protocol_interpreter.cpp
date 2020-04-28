#include "network/itp/itp_protocol_interpreter.h"

#include <cstdint>
#include <exception>
#include <memory>

#include "loggers/network_logger.h"
#include "network/itp/itp_command_factory.h"
#include "network/itp/itp_network_commands.h"
#include "network/itp/itp_packet_writer.h"
#include "network/network_defs.h"
#include "network/network_io_utils.h"

namespace terrier {
namespace network {
class ConnectionContext;
}  // namespace network
namespace trafficcop {
class TrafficCop;
}  // namespace trafficcop
}  // namespace terrier

namespace terrier::network {
Transition ITPProtocolInterpreter::Process(common::ManagedPointer<ReadBuffer> in,
                                           common::ManagedPointer<WriteQueue> out,
                                           common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                           common::ManagedPointer<ConnectionContext> context) {
  try {
    if (!TryBuildPacket(in)) return Transition::NEED_READ_TIMEOUT;
  } catch (std::exception &e) {
    NETWORK_LOG_ERROR("Encountered exception {0} when parsing packet", e.what());
    return Transition::TERMINATE;
  }
  auto command = command_factory_->PacketToCommand(common::ManagedPointer(&curr_input_packet_));
  ITPPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
                                 common::ManagedPointer<ITPPacketWriter>(&writer), t_cop, context);
  curr_input_packet_.Clear();
  return ret;
}

void ITPProtocolInterpreter::GetResult(const common::ManagedPointer<WriteQueue> out) {
  ITPPacketWriter writer(out);
  writer.WriteCommandComplete();
}

size_t ITPProtocolInterpreter::GetPacketHeaderSize() { return 1 + sizeof(uint32_t); }

void ITPProtocolInterpreter::SetPacketMessageType(const common::ManagedPointer<ReadBuffer> in) {
  curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
}

}  // namespace terrier::network
