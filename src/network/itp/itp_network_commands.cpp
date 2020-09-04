#include "network/itp/itp_network_commands.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_network_commands.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "traffic_cop/traffic_cop.h"
#include "type/type_id.h"

namespace terrier::network {

  Transition ReplicationCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                      common::ManagedPointer<ITPPacketWriter> out,
                                      common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                      common::ManagedPointer<ConnectionContext> connection) {
    // TODO(Gus): Figure out what to do with message_id
    auto message_id UNUSED_ATTRIBUTE = in_.ReadValue<uint64_t>();
    auto data_size = in_.ReadValue<uint64_t>();
    TERRIER_ASSERT(in_.HasMore(data_size), "Insufficient replication data in packet");
    auto buffer = std::make_unique<ReadBuffer>(data_size);
    buffer->FillBufferFrom(in_, data_size);
    t_cop->HandBufferToReplication(std::move(buffer));
    return Transition::PROCEED;
  }

  Transition StopReplicationCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                          common::ManagedPointer<ITPPacketWriter> out,
                                          common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                          common::ManagedPointer<ConnectionContext> connection) {
    t_cop->StopReplication();
    return Transition::PROCEED;
  }

}  // namespace terrier::network