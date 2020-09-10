#include "network/itp/itp_protocol_interpreter.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/itp/itp_network_commands.h"
#include "network/network_defs.h"
#include "network/terrier_server.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

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

  std::shared_ptr<ITPNetworkCommand> command =
      command_factory_->PacketToCommand(common::ManagedPointer<InputPacket>(&curr_input_packet_));
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

bool ITPProtocolInterpreter::TryBuildPacket(common::ManagedPointer<ReadBuffer> in) {
  if (!TryReadPacketHeader(in)) return false;

  size_t size_needed = curr_input_packet_.extended_
                           ? curr_input_packet_.len_ - curr_input_packet_.buf_->BytesAvailable()
                           : curr_input_packet_.len_;

  size_t can_read = std::min(size_needed, in->BytesAvailable());
  size_t remaining_bytes = size_needed - can_read;

  // copy bytes only if the packet is longer than the read buffer,
  // otherwise we can use the read buffer to save space
  if (curr_input_packet_.extended_) {
    curr_input_packet_.buf_->FillBufferFrom(in, can_read);
  }

  return remaining_bytes <= 0;
}

bool ITPProtocolInterpreter::TryReadPacketHeader(common::ManagedPointer<ReadBuffer> in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message type + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = sizeof(NetworkMessageType) + sizeof(int32_t);
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
    // Allocate a larger buffer to copy bytes off from the I/O layer's buffer
    ReadBuffer buffer(curr_input_packet_.len_);
    curr_input_packet_.buf_ = common::ManagedPointer<ReadBuffer>(&buffer).Get();
    NETWORK_LOG_TRACE("Extended Buffer size required for packet of size {0}", curr_input_packet_.len_)
    curr_input_packet_.extended_ = true;
  } else {
    curr_input_packet_.buf_ = in.Get();
  }

  curr_input_packet_.header_parsed_ = true;
  return true;
}

}  // namespace terrier::network
