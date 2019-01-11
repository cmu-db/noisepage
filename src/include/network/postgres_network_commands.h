#pragma once
#include <utility>
#include "common/macros.h"
#include "loggers/main_logger.h"
#include "network/network_defs.h"
#include "network/network_types.h"
#include "network/postgres_protocol_utils.h"
#include "type/value_factory.h"

#define DEFINE_COMMAND(name, flush)                                                              \
  class name : public PostgresNetworkCommand {                                                   \
   public:                                                                                       \
    explicit name(PostgresInputPacket *in) : PostgresNetworkCommand(in, flush) {}                \
    Transition Exec(PostgresProtocolInterpreter *interpreter, PostgresPacketWriter *out, \
                            CallbackFunc callback) override;                                     \
  }

namespace terrier::network {

class PostgresProtocolInterpreter;

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolInterpreter *interpreter, PostgresPacketWriter *out,
                          CallbackFunc callback) = 0;

  bool FlushOnComplete() { return flush_on_complete_; }

  virtual ~PostgresNetworkCommand() = default;

 protected:
  explicit PostgresNetworkCommand(PostgresInputPacket *in, bool flush)
      : in_(in->buf_->ReadIntoView(in->len_)), flush_on_complete_(flush) {}

  ReadBufferView in_;

 private:
  bool flush_on_complete_;
};

DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, false);
DEFINE_COMMAND(BindCommand, false);
DEFINE_COMMAND(DescribeCommand, false);
DEFINE_COMMAND(ExecuteCommand, false);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, false);
DEFINE_COMMAND(TerminateCommand, true);

}  // namespace terrier::network
