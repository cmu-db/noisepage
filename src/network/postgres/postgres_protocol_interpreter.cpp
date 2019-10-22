#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/network_defs.h"
#include "network/postgres/postgres_network_commands.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace terrier::network {
Transition PostgresProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                                common::ManagedPointer<ConnectionContext> context,
                                                NetworkCallback callback) {
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
  std::shared_ptr<PostgresNetworkCommand> command = command_factory_->PacketToCommand(&curr_input_packet_);
  PostgresPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
                                 common::ManagedPointer<PostgresPacketWriter>(&writer), t_cop, context, callback);
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
    writer.WriteSSLPacket(NetworkMessageType::PG_SSL_NO);
    return Transition::PROCEED;
  }

  // Process startup packet
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    NETWORK_LOG_TRACE("Protocol error: only protocol version 3 is supported");
    writer.WriteErrorResponse({{NetworkMessageType::PG_HUMAN_READABLE_ERROR, "Protocol Version Not Supported"}});
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

size_t PostgresProtocolInterpreter::GetPacketHeaderSize() { return startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t); }

void PostgresProtocolInterpreter::SetPacketMessageType(const std::shared_ptr<ReadBuffer> &in) {
  if (!startup_) curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
}

void PostgresProtocolInterpreter::CompleteCommand(PostgresPacketWriter *const out, const QueryType &query_type,
                                                  int rows) {
  out->BeginPacket(NetworkMessageType::PG_COMMAND_COMPLETE).EndPacket();
}

void PostgresProtocolInterpreter::ExecQueryMessageGetResult(PostgresPacketWriter *const out, ResultType status) {
  CompleteCommand(out, QueryType::QUERY_INVALID, 0);

  out->WriteReadyForQuery(NetworkTransactionStateType::IDLE);
}

void PostgresProtocolInterpreter::ExecExecuteMessageGetResult(PostgresPacketWriter *const out, ResultType status) {
  CompleteCommand(out, QueryType::QUERY_INVALID, 0);
}

}  // namespace terrier::network
