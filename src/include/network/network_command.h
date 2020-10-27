#pragma once

#include <utility>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "network/connection_context.h"
#include "network/network_defs.h"
#include "network/network_types.h"
#include "network/packet_writer.h"
#include "network/protocol_interpreter.h"

namespace noisepage::network {

/**
 * Interface for the execution of a NetworkCommand
 */
class NetworkCommand {
 public:
  /**
   * @return Whether or not to flush the output network packets from this on completion
   */
  bool FlushOnComplete() { return flush_on_complete_; }

  /**
   * Default destructor
   */
  virtual ~NetworkCommand() = default;

 protected:
  /**
   * Constructor for a NetworkCommand instance
   * @param in The input packets to this command
   * @param flush Whether or not to flush the outuput packets on completion
   */
  NetworkCommand(const common::ManagedPointer<InputPacket> in, bool flush)
      : in_(in->buf_->ReadIntoView(in->len_)), flush_on_complete_(flush) {}

  /**
   * The ReadBufferView to read input packets from
   */
  ReadBufferView in_;

 private:
  bool flush_on_complete_;
};
}  // namespace noisepage::network
