#include "network/itp/itp_network_commands.h"

#include <memory>
#include <type_traits>

#include "traffic_cop/traffic_cop.h"

namespace terrier::network {
class ConnectionContext;
class ITPPacketWriter;
class ProtocolInterpreter;
}  // namespace terrier::network

namespace terrier::network {

Transition ReplicationCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                    common::ManagedPointer<ITPPacketWriter> out,
                                    common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                    common::ManagedPointer<ConnectionContext> connection) {
  std::unique_ptr<ReadBuffer> buffer;
  buffer->FillBufferFrom(in_, in_len_);
  t_cop->HandBufferToReplication(std::move(buffer));
  return Transition::PROCEED;
}

Transition StopReplicationCommand::Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                                        common::ManagedPointer<ITPPacketWriter> out,
                                        common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                        common::ManagedPointer<ConnectionContext> connection) {
  // TODO(Tianlei): modify this implementation for the complete ITP protocol
  return Transition::PROCEED;
}

}  // namespace terrier::network
