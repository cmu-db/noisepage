#include "network/postgres/postgres_protocol_interpreter.h"

#include <algorithm>
#include <string>
#include <thread>  // NOLINT
#include <utility>

#include "common/error/error_data.h"
#include "common/error/error_defs.h"
#include "network/network_defs.h"
#include "network/postgres/postgres_network_commands.h"
#include "traffic_cop/traffic_cop.h"

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

  if (WaitingForSync() && curr_input_packet_.msg_type_ != NetworkMessageType::PG_SYNC_COMMAND) {
    // When an error is detected while processing any Extended Query message, the backend issues ErrorResponse, then
    // reads and discards messages until a Sync is reached
    curr_input_packet_.Clear();
    return Transition::PROCEED;
  }

  const Transition ret = command->Exec(common::ManagedPointer<ProtocolInterpreter>(this),
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
    writer.WriteError({common::ErrorSeverity::FATAL,
                       fmt::format("Protocol error: only protocol version 3 is supported. Received protocol version {}",
                                   PROTO_MAJOR_VERSION(proto_version)),
                       common::ErrorCode::ERRCODE_CONNECTION_FAILURE});
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

  std::pair<catalog::db_oid_t, catalog::namespace_oid_t> oids;

  uint32_t sleep_time = INITIAL_BACKOFF_TIME;

  // we loop with exponential backoff because in case there are multiple other pg connections also starting
  // at the same time, creating DDL conflicts when creating the temp namespace
  do {
    oids = t_cop->CreateTempNamespace(context->GetConnectionID(), db_name);
    if (oids.first == catalog::INVALID_DATABASE_OID || oids.second != catalog::INVALID_NAMESPACE_OID) break;
    std::this_thread::sleep_for(std::chrono::milliseconds{sleep_time});
    sleep_time *= BACKOFF_FACTOR;
  } while (sleep_time <= MAX_BACKOFF_TIME);

  if (oids.first == catalog::INVALID_DATABASE_OID) {
    // Invalid database name
    writer.WriteError({common::ErrorSeverity::FATAL, fmt::format("Database \"{}\" does not exist", db_name),
                       common::ErrorCode::ERRCODE_UNDEFINED_DATABASE});
    return Transition::TERMINATE;
  }
  if (oids.second == catalog::INVALID_NAMESPACE_OID) {
    // Failed to create temporary namespace. Client should retry.
    writer.WriteError({common::ErrorSeverity::FATAL,
                       "Failed to create a temporary namespace for this connection. There may be a concurrent "
                       "DDL change. Please retry.",
                       common::ErrorCode::ERRCODE_CONNECTION_FAILURE});
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
  // Close any open transaction
  if (context->Transaction() != nullptr) {
    t_cop->EndTransaction(context, QueryType::QUERY_ROLLBACK);
    // We're about to destruct this object (probably), but reset state anyway
    ResetTransactionState();
  }

  // Drop the temp namespace (if it exists) for this connection.

  // It's possible that the client provided an invalid database name, in which case there's nothing to do
  if (context->GetDatabaseOid() == catalog::INVALID_DATABASE_OID) {
    return;
  }

  // It's possible that temporary namespace failed to be
  // created and we're closing the connection for that reason, in
  // case there's nothing to drop
  if (context->GetTempNamespaceOid() != catalog::INVALID_NAMESPACE_OID) {
    while (!t_cop->DropTempNamespace(context->GetDatabaseOid(), context->GetTempNamespaceOid())) {
    }
  }
}

size_t PostgresProtocolInterpreter::GetPacketHeaderSize() { return startup_ ? sizeof(uint32_t) : 1 + sizeof(uint32_t); }

void PostgresProtocolInterpreter::SetPacketMessageType(const common::ManagedPointer<ReadBuffer> in) {
  if (!startup_) curr_input_packet_.msg_type_ = in->ReadValue<NetworkMessageType>();
}

}  // namespace terrier::network
