#pragma once
#include <utility>
#include "common/macros.h"
#include "network/network_defs.h"
#include "network/network_types.h"
#include "network/postgres_protocol_utils.h"

#define DEFINE_COMMAND(name, flush)                                                      \
  class name : public PostgresNetworkCommand {                                           \
   public:                                                                               \
    explicit name(PostgresInputPacket *in) : PostgresNetworkCommand(in, flush) {}        \
    Transition Exec(PostgresProtocolInterpreter *interpreter, PostgresPacketWriter *out, \
                    CallbackFunc callback) override;                                     \
  }

namespace terrier::network {

class PostgresProtocolInterpreter;

/**
 * Interface for the execution of the standard PostgresNetworkCommands for the postgres protocol
 */
class PostgresNetworkCommand {
 public:
  /**
   * Executes the command
   * @param interpreter The protocol interpreter that called this
   * @param out The Writer on which to construct output packets for the client
   * @param callback The callback function to trigger after
   * @return The next transition for the client's state machine
   */
  virtual Transition Exec(PostgresProtocolInterpreter *interpreter, PostgresPacketWriter *out,
                          CallbackFunc callback) = 0;

  /**
   * @return Whether or not to flush the output network packets from this on completion
   */
  bool FlushOnComplete() { return flush_on_complete_; }

  /**
   * Default destructor
   */
  virtual ~PostgresNetworkCommand() = default;

 protected:
  /**
   * Constructor for a NetworkCommand instance
   * @param in The input packets to this command
   * @param flush Whether or not to flush the outuput packets on completion
   */
  explicit PostgresNetworkCommand(PostgresInputPacket *in, bool flush)
      : in_(in->buf_->ReadIntoView(in->len_)), flush_on_complete_(flush) {}

  /**
   * The ReadBufferView to read input packets from
   */
  ReadBufferView in_;

 private:
  bool flush_on_complete_;
};

// Set all to force flush for now
DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, true);
DEFINE_COMMAND(BindCommand, true);
DEFINE_COMMAND(DescribeCommand, true);
DEFINE_COMMAND(ExecuteCommand, true);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, true);
DEFINE_COMMAND(TerminateCommand, true);

}  // namespace terrier::network
