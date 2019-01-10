#pragma once
#include <utility>
#include "type/value_factory.h"
#include "network/network_defs.h"
#include "loggers/main_logger.h"
#include "common/macros.h"
#include "network/network_types.h"
#include "network/postgres_protocol_utils.h"

#define DEFINE_COMMAND(name, flush)                                        \
class name : public PostgresNetworkCommand {                               \
 public:                                                                   \
  explicit name(PostgresInputPacket &in)                                   \
     : PostgresNetworkCommand(in, flush) {}                                \
  virtual Transition Exec(PostgresProtocolInterpreter &,                   \
                          PostgresPacketWriter &,                          \
                          CallbackFunc) override;                          \
}

 namespace terrier {
 namespace network {

 class PostgresProtocolInterpreter;

 class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolInterpreter &interpreter,
                          PostgresPacketWriter &out,
                          CallbackFunc callback) = 0;

  inline bool FlushOnComplete() { return flush_on_complete_; }

  virtual ~PostgresNetworkCommand(){};

 protected:
  explicit PostgresNetworkCommand(PostgresInputPacket &in, bool flush)
      : in_(in.buf_->ReadIntoView(in.len_)), flush_on_complete_(flush) {}

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

} // namespace network
} // namespace terrier
