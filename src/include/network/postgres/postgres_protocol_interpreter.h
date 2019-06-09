#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/connection_handle.h"
#include "network/postgres/postgres_network_commands.h"
#include "network/postgres_command_factory.h"
#include "network/protocol_interpreter.h"

namespace terrier::network {

/**
 * Interprets the network protocol for postgres clients
 */
class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  struct Provider : public ProtocolInterpreter::Provider {
   public:
    explicit Provider(common::ManagedPointer<PostgresCommandFactory> command_factory)
        : command_factory_(command_factory) {}

    std::unique_ptr<ProtocolInterpreter> Get() override {
      return std::make_unique<PostgresProtocolInterpreter>(command_factory_);
    }

   private:
    common::ManagedPointer<PostgresCommandFactory> command_factory_;
  };

  /**
   * Default constructor
   */
  explicit PostgresProtocolInterpreter(common::ManagedPointer<PostgresCommandFactory> command_factory)
      : command_factory_(command_factory) {}
  /**
   * @see ProtocolIntepreter::Process
   * @param in
   * @param out
   * @param callback
   * @param t_cop the traffic cop pointer
   * @param context the connection context
   * @return
   */
  Transition Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                     common::ManagedPointer<tcop::TrafficCop> t_cop, common::ManagedPointer<ConnectionContext> context,
                     NetworkCallback callback) override;

  /**
   *
   * @param out
   */
  void GetResult(std::shared_ptr<WriteQueue> out) override {
    PostgresPacketWriter writer(out);
    ExecQueryMessageGetResult(&writer, ResultType::SUCCESS);
    // TODO(Tianyu): This looks wrong. JDBC and PSQL should be united under one wire protocol. This field was set
    // statically for all connections before I removed it. Also, the difference between these two methods look
    // superficial. Some one should dig deeper to figure out what's going on here.
    //    switch (protocol_type_) {
    //      case NetworkProtocolType::POSTGRES_JDBC:NETWORK_LOG_TRACE("JDBC result");
    //        ExecExecuteMessageGetResult(&writer, ResultType::SUCCESS);
    //        break;
    //      case NetworkProtocolType::POSTGRES_PSQL:NETWORK_LOG_TRACE("PSQL result");
    //        ExecQueryMessageGetResult(&writer, ResultType::SUCCESS);
    //      default:throw NETWORK_PROCESS_EXCEPTION("Unsupported protocol type");
    //    }
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

 private:
  bool startup_ = true;
  PostgresInputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;
  common::ManagedPointer<PostgresCommandFactory> command_factory_;

  bool TryBuildPacket(const std::shared_ptr<ReadBuffer> &in);
  bool TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in);
};

}  // namespace terrier::network
