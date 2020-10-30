#include "network/itp/itp_network_commands.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "network/itp/itp_protocol_interpreter.h"
#include "network/noisepage_server.h"
#include "traffic_cop/traffic_cop.h"
#include "type/type_id.h"

namespace noisepage::network {

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

}  // namespace noisepage::network
