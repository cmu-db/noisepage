#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/network_defs.h"
#include "network/itp/itp_network_commands.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "network/terrier_server.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace terrier::network {
Transition ITPProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                                common::ManagedPointer<ConnectionContext> context,
                                                NetworkCallback callback) {
  try {
    if (!TryBuildPacket(in, &curr_input_packet_)) return Transition::NEED_READ_TIMEOUT;
  } catch (std::exception &e) {
    NETWORK_LOG_ERROR("Encountered exception {0} when parsing packet", e.what());
    return Transition::TERMINATE;
  }
  if (startup_) {
    // Always flush startup packet response
    out->ForceFlush();
    curr_input_packet_.Clear();
    return ProcessStartup(in, out);
  }
  std::shared_ptr<ITPNetworkCommand> command = command_factory_->PacketToCommand(&curr_input_packet_);
  ITPPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
                                 common::ManagedPointer<ITPPacketWriter>(&writer), t_cop, context, callback);
  curr_input_packet_.Clear();
  return ret;
}

void ITPProtocolInterpreter::GetResult(std::shared_ptr<WriteQueue> out) {
  ITPPacketWriter writer(out);
  out->BeginPacket(NetworkMessageType::ITP_COMMAND_COMPLETE).EndPacket();
}

bool ITPProtocolInterpreter::TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = 1 + sizeof(int32_t);
  // Make sure the entire header is readable
  if (!in->HasMore(header_size)) return false;

  // The header is ready to be read, fill in fields accordingly
  curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
  curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);
  if (curr_input_packet_.len_ > PACKET_LEN_LIMIT) {
    NETWORK_LOG_ERROR("Packet size {} > limit {}", curr_input_packet_.len_, PACKET_LEN_LIMIT);
    throw NETWORK_PROCESS_EXCEPTION("Packet too large");
  }

  // Extend the buffer as needed
  if (curr_input_packet_.len_ > in->Capacity()) {
    // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
    curr_input_packet_.buf_ = std::make_shared<ReadBuffer>(curr_input_packet_.len_);
    NETWORK_LOG_TRACE("Extended Buffer size required for packet of size {0}", curr_input_packet_.len_);
    curr_input_packet_.extended_ = true;
  } else {
    curr_input_packet_.buf_ = in;
  }

  curr_input_packet_.header_parsed_ = true;
  return true;
}

}  // namespace terrier::network
