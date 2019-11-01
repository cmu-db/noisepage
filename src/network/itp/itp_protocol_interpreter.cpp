#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/itp/itp_network_commands.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "network/network_defs.h"
#include "network/terrier_server.h"

namespace terrier::network {
Transition ITPProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                                           common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                           common::ManagedPointer<ConnectionContext> context,
                                           NetworkCallback callback) {
  try {
    if (!TryBuildPacket(in)) return Transition::NEED_READ_TIMEOUT;
  } catch (std::exception &e) {
    NETWORK_LOG_ERROR("Encountered exception {0} when parsing packet", e.what());
    return Transition::TERMINATE;
  }
  std::unique_ptr<ITPNetworkCommand> command = command_factory_->PacketToCommand(&curr_input_packet_);
  ITPPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
                                 common::ManagedPointer<ITPPacketWriter>(&writer), t_cop, context, callback);
  curr_input_packet_.Clear();
  return ret;
}

void ITPProtocolInterpreter::GetResult(std::shared_ptr<WriteQueue> out) {
  ITPPacketWriter writer(out);
  writer.WriteCommandComplete();
}

size_t ITPProtocolInterpreter::GetPacketHeaderSize() { return 1 + sizeof(uint32_t); }

void ITPProtocolInterpreter::SetPacketMessageType(const std::shared_ptr<ReadBuffer> &in) {
  curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
}

}  // namespace terrier::network
