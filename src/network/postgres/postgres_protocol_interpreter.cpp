#include "network/postgres/postgres_protocol_interpreter.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "network/network_defs.h"
#include "network/postgres/postgres_network_commands.h"
#include "network/terrier_server.h"

constexpr uint32_t SSL_MESSAGE_VERNO = 80877103;
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace terrier::network {
Transition PostgresProtocolInterpreter::Process(common::ManagedPointer<ReadBuffer> in,
                                                common::ManagedPointer<WriteQueue> out,
                                                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                                common::ManagedPointer<ConnectionContext> context) {
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
    return ProcessStartup(in, out, t_cop, context);
  }
  auto command = command_factory_->PacketToCommand(common::ManagedPointer<InputPacket>(&curr_input_packet_));
  PostgresPacketWriter writer(out);
  if (command->FlushOnComplete()) out->ForceFlush();
  Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
                                 common::ManagedPointer<PostgresPacketWriter>(&writer), t_cop, context);
  curr_input_packet_.Clear();
  return ret;
}

Transition PostgresProtocolInterpreter::ProcessStartup(const common::ManagedPointer<ReadBuffer> in,
                                                       const common::ManagedPointer<WriteQueue> out,
                                                       const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                                       const common::ManagedPointer<ConnectionContext> context) {
  PostgresPacketWriter writer(out);
  auto proto_version = in->ReadValue<uint32_t>();
  NETWORK_LOG_TRACE("protocol version: {0}", proto_version);

  if (proto_version == SSL_MESSAGE_VERNO) {
    // We don't support SSL yet. Reply with a response telling the client not to use SSL.
    writer.WriteType(static_cast<NetworkMessageType>('N'));
    return Transition::PROCEED;
  }

  // Process startup packet
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    NETWORK_LOG_TRACE("Protocol error: only protocol version 3 is supported");
    writer.WriteErrorResponse("Protocol Version Not Supported");
    return Transition::TERMINATE;
  }

  // The last bit of the packet will be nul. This is not a valid field. When there
  // is less than 2 bytes of data remaining we can already exit early.
  while (in->HasMore(2)) {
    // TODO(Tianyu): We don't seem to really handle the other flags?
    std::string key = in->ReadString(), value = in->ReadString();
    NETWORK_LOG_TRACE("Option key {0}, value {1}", key.c_str(), value.c_str());
    if (key == std::string("database")) {
      context->CommandLineArgs()[key] = std::move(value);
    }
  }
  // skip the last nul byte
  in->Skip(1);
  // TODO(Tianyu): Implement authentication. For now we always send AuthOK

  // Create a temp namespace for this connection
  std::string db_name = catalog::DEFAULT_DATABASE;
  auto &cmdline_args = context->CommandLineArgs();
  if (cmdline_args.find("database") != cmdline_args.end()) {
    if (!cmdline_args["database"].empty()) {
      db_name = cmdline_args["database"];
      NETWORK_LOG_TRACE(db_name);
    }
  }
  auto oids = t_cop->CreateTempNamespace(context->GetConnectionID(), db_name);
  if (oids.first == catalog::INVALID_DATABASE_OID) {
    // Invalid database name
    // TODO(Matt): need to actually return an error to the client
    return Transition::TERMINATE;
  }
  if (oids.second == catalog::INVALID_NAMESPACE_OID) {
    // Failed to create temporary namespace. Client should retry.
    // TODO(Matt): need to actually return an error to the client
    return Transition::TERMINATE;
  }

  // Temp namespace creation succeeded, stash some metadata about it in the ConnectionContext
  context->SetDatabaseName(std::move(db_name));
  context->SetDatabaseOid(oids.first);
  context->SetTempNamespaceOid(oids.second);

  // All done
  writer.WriteStartupResponse();
  startup_ = false;
  return Transition::PROCEED;
}

void PostgresProtocolInterpreter::Teardown(const common::ManagedPointer<ReadBuffer> in,
                                           const common::ManagedPointer<WriteQueue> out,
                                           const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                           const common::ManagedPointer<ConnectionContext> context) {
  // Drop the temp namespace (if it exists) for this connection. It's possible that temporary namespace failed to be
  // created and we're closing the connection for that reason, in
  // case there's nothing to drop
  if (context->GetTempNamespaceOid() != catalog::INVALID_NAMESPACE_OID) {
    while (!t_cop->DropTempNamespace(context->GetTempNamespaceOid(), context->GetDatabaseOid())) {
    }
  }
}

size_t PostgresProtocolInterpreter::GetPacketHeaderSize() { return startup_ ? sizeof(uint32_t) : 1 + sizeof(uint32_t); }

void PostgresProtocolInterpreter::SetPacketMessageType(const common::ManagedPointer<ReadBuffer> in) {
  if (!startup_) curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
}

}  // namespace terrier::network
