#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "loggers/network_logger.h"
#include "network/postgres_network_commands.h"
#include "network/protocol_interpreter.h"

namespace terrier::network {

/**
 * Interprets the network protocol for postgres clients
 */
class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  /**
   * Default constructor
   */
  PostgresProtocolInterpreter() = default;
  /**
   * @see ProtocolIntepreter::Process
   * @param in
   * @param out
   * @param callback
   * @return
   */
  Transition Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out, CallbackFunc callback) override;

  /**
   *
   * @param out
   */
  void GetResult(std::shared_ptr<WriteQueue> out) override {
    // TODO(Tianyu): The difference between these two methods are unclear to me

    PostgresPacketWriter writer(out);
    switch (protocol_type_) {
      case NetworkProtocolType::POSTGRES_JDBC:
        NETWORK_LOG_TRACE("JDBC result");
        ExecExecuteMessageGetResult(&writer, ResultType::SUCCESS);
        break;
      case NetworkProtocolType::POSTGRES_PSQL:
        NETWORK_LOG_TRACE("PSQL result");
        ExecQueryMessageGetResult(&writer, ResultType::SUCCESS);
      default:
        throw NETWORK_PROCESS_EXCEPTION("Unsupported protocol type");
    }
  }

  /**
   *
   * @param in
   * @param out
   * @return
   */
  Transition ProcessStartup(const std::shared_ptr<ReadBuffer> &in, const std::shared_ptr<WriteQueue> &out);

  /**
   *
   * @param out
   * @param query_type
   * @param rows
   */
  void CompleteCommand(PostgresPacketWriter *out, const QueryType &query_type, int rows);

  /**
   *
   * @param out
   * @param status
   */
  void ExecQueryMessageGetResult(PostgresPacketWriter *out, ResultType status);

  /**
   *
   * @param out
   * @param status
   */
  void ExecExecuteMessageGetResult(PostgresPacketWriter *out, ResultType status);

  /**
   * The protocol type being used
   */
  NetworkProtocolType protocol_type_;

 private:
  bool startup_ = true;
  PostgresInputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;
  bool TryBuildPacket(const std::shared_ptr<ReadBuffer> &in);
  bool TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in);
  std::shared_ptr<PostgresNetworkCommand> PacketToCommand();
};

}  // namespace terrier::network
