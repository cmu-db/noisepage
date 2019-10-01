#pragma once
#include <utility>
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "network/abstract_packet_writer.h"
#include "network/connection_context.h"
#include "network/network_defs.h"
#include "network/network_types.h"
#include "network/protocol_interpreter.h"

namespace terrier::network {

/**
 * Interface for the execution of the standard AbstractNetworkCommands for the postgres protocol
 */
class AbstractNetworkCommand {
 public:
  /**
   * @return Whether or not to flush the output network packets from this on completion
   */
  bool FlushOnComplete() { return flush_on_complete_; }

  /**
   * Default destructor
   */
  virtual ~AbstractNetworkCommand() = default;

  /**
   * Executes the command
   * @param interpreter The protocol interpreter that called this
   * @param out The Writer on which to construct output packets for the client
   * @param t_cop The traffic cop pointer
   * @param connection The ConnectionContext which contains connection information
   * @param callback The callback function to trigger after
   * @return The next transition for the client's state machine
   */
  virtual Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                          common::ManagedPointer<AbstractPacketWriter> out,
                          common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                          common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) = 0;

 protected:
  /**
   * Constructor for a NetworkCommand instance
   * @param in The input packets to this command
   * @param flush Whether or not to flush the outuput packets on completion
   */
  AbstractNetworkCommand(InputPacket *in, bool flush)
      : in_(in->buf_->ReadIntoView(in->len_)), flush_on_complete_(flush) {}

  /**
   * The ReadBufferView to read input packets from
   */
  ReadBufferView in_;

 private:
  bool flush_on_complete_;
};
}  // namespace terrier::network
