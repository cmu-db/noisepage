#pragma once
#include <utility>
#include "loggers/main_logger.h"
#include "network/protocol_interpreter.h"
#include "network/postgres_network_commands.h"

 namespace terrier::network {

 class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  // TODO(Tianyu): Is this even the right thread id? It seems that all the
  explicit PostgresProtocolInterpreter(size_t thread_id) {
    //state_.thread_id_= thread_id;
  };

  Transition Process(std::shared_ptr<ReadBuffer> in,
                     std::shared_ptr<WriteQueue> out,
                     CallbackFunc callback) override;

  inline void GetResult(std::shared_ptr<WriteQueue> out) override {

    // TODO(Tianyu): The difference between these two methods are unclear to me

    PostgresPacketWriter writer(*out);
    switch (protocol_type_) {
      case NetworkProtocolType::POSTGRES_JDBC:
        LOG_TRACE("JDBC result");
        ExecExecuteMessageGetResult(writer, ResultType::SUCCESS);
        break;
      case NetworkProtocolType::POSTGRES_PSQL:
        LOG_TRACE("PSQL result");
        ExecQueryMessageGetResult(writer, ResultType::SUCCESS);
    }
  }

  Transition ProcessStartup(std::shared_ptr<ReadBuffer> in,
                           std::shared_ptr<WriteQueue> out);



  // TODO(Tianyu): Remove these later for better responsibility assignment
  bool HardcodedExecuteFilter(QueryType query_type);
  void CompleteCommand(PostgresPacketWriter &out, const QueryType &query_type, int rows);
  void ExecQueryMessageGetResult(PostgresPacketWriter &out, ResultType status);
  void ExecExecuteMessageGetResult(PostgresPacketWriter &out, ResultType status);

  NetworkProtocolType protocol_type_;

 private:
  bool startup_ = true;
  PostgresInputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;
  //tcop::ClientProcessState state_;
  bool TryBuildPacket(std::shared_ptr<ReadBuffer> &in);
  bool TryReadPacketHeader(std::shared_ptr<ReadBuffer> &in);
  std::shared_ptr<PostgresNetworkCommand> PacketToCommand();
};

} // namespace terrier::network
