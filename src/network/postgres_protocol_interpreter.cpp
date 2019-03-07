#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/network_defs.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"

#define MAKE_COMMAND(type) \
  std::static_pointer_cast<PostgresNetworkCommand, type>(std::make_shared<type>(&curr_input_packet_))
#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace terrier::network {
Transition PostgresProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                                                CallbackFunc callback) {
  try {
    if (!TryBuildPacket(in)) return Transition::NEED_READ_TIMEOUT;
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
  std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
  PostgresPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(this, &writer, callback);
  curr_input_packet_.Clear();
  return ret;
}

Transition PostgresProtocolInterpreter::ProcessStartup(const std::shared_ptr<ReadBuffer> &in,
                                                       const std::shared_ptr<WriteQueue> &out) {
  PostgresPacketWriter writer(out);
  auto proto_version = in->ReadValue<uint32_t>();
  NETWORK_LOG_TRACE("protocol version: {0}", proto_version);

  if (proto_version == SSL_MESSAGE_VERNO) {
    // TODO(Tianyu): Should this be moved from PelotonServer into settings?
    writer.WriteSingleTypePacket(NetworkMessageType::SSL_NO);
    return Transition::PROCEED;
  }

  // Process startup packet
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    NETWORK_LOG_TRACE("Protocol error: only protocol version 3 is supported");
    writer.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR, "Protocol Version Not Supported"}});
    return Transition::TERMINATE;
  }

  // The last bit of the packet will be nul. This is not a valid field. When there
  // is less than 2 bytes of data remaining we can already exit early.
  while (in->HasMore(2)) {
    // TODO(Tianyu): We don't seem to really handle the other flags?
    std::string key = in->ReadString(), value = in->ReadString();
    NETWORK_LOG_TRACE("Option key {0}, value {1}", key.c_str(), value.c_str());
    if (key == std::string("database"))
      // state_.db_name_ = value;
      cmdline_options_[key] = std::move(value);
  }
  // skip the last nul byte
  in->Skip(1);
  // TODO(Tianyu): Implement authentication. For now we always send AuthOK
  writer.WriteStartupResponse();
  startup_ = false;
  return Transition::PROCEED;
}

bool PostgresProtocolInterpreter::TryBuildPacket(const std::shared_ptr<ReadBuffer> &in) {
  if (!TryReadPacketHeader(in)) return false;

  size_t size_needed = curr_input_packet_.extended_
                           ? curr_input_packet_.len_ - curr_input_packet_.buf_->BytesAvailable()
                           : curr_input_packet_.len_;

  size_t can_read = std::min(size_needed, in->BytesAvailable());
  size_t remaining_bytes = size_needed - can_read;

  // copy bytes only if the packet is longer than the read buffer,
  // otherwise we can use the read buffer to save space
  if (curr_input_packet_.extended_) {
    curr_input_packet_.buf_->FillBufferFrom(*in, can_read);
  }

  return remaining_bytes <= 0;
}

bool PostgresProtocolInterpreter::TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message type (only if non-startup)
  //              + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
  // Make sure the entire header is readable
  if (!in->HasMore(header_size)) return false;

  // The header is ready to be read, fill in fields accordingly
  if (!startup_) curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
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

std::shared_ptr<PostgresNetworkCommand> PostgresProtocolInterpreter::PacketToCommand() {
  switch (curr_input_packet_.msg_type_) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND:
      return MAKE_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PARSE_COMMAND:
      return MAKE_COMMAND(ParseCommand);
    case NetworkMessageType::BIND_COMMAND:
      return MAKE_COMMAND(BindCommand);
    case NetworkMessageType::DESCRIBE_COMMAND:
      return MAKE_COMMAND(DescribeCommand);
    case NetworkMessageType::EXECUTE_COMMAND:
      return MAKE_COMMAND(ExecuteCommand);
    case NetworkMessageType::SYNC_COMMAND:
      return MAKE_COMMAND(SyncCommand);
    case NetworkMessageType::CLOSE_COMMAND:
      return MAKE_COMMAND(CloseCommand);
    case NetworkMessageType::TERMINATE_COMMAND:
      return MAKE_COMMAND(TerminateCommand);
    default:
      throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
  }
}

void PostgresProtocolInterpreter::CompleteCommand(PostgresPacketWriter *const out, const QueryType &query_type,
                                                  int rows) {
  out->BeginPacket(NetworkMessageType::COMMAND_COMPLETE).EndPacket();
}

void PostgresProtocolInterpreter::ExecQueryMessageGetResult(PostgresPacketWriter *const out, ResultType status) {
  CompleteCommand(out, QueryType::QUERY_INVALID, 0);

  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
}

void PostgresProtocolInterpreter::ExecExecuteMessageGetResult(PostgresPacketWriter *const out, ResultType status) {
  CompleteCommand(out, QueryType::QUERY_INVALID, 0);
}

}  // namespace terrier::network
