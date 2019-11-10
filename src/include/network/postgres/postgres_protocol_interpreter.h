#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/connection_handle.h"
#include "network/postgres/postgres_command_factory.h"
#include "network/postgres/postgres_network_commands.h"
#include "network/postgres/postgres_packet_writer.h"
#include "network/protocol_interpreter.h"

namespace terrier::network {

/**
 * Interprets the network protocol for postgres clients
 */
class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  /**
   * The provider encapsulates the creation logic of a protocol interpreter into an object
   */
  struct Provider : public ProtocolInterpreter::Provider {
   public:
    /**
     * Constructs a new provider
     * @param command_factory The command factory to use for the constructed protocol interpreters
     */
    explicit Provider(common::ManagedPointer<PostgresCommandFactory> command_factory)
        : command_factory_(command_factory) {}

    /**
     * @return an instance of the protocol interpreter
     */
    std::unique_ptr<ProtocolInterpreter> Get() override {
      return std::make_unique<PostgresProtocolInterpreter>(command_factory_);
    }

   private:
    common::ManagedPointer<PostgresCommandFactory> command_factory_;
  };

  /**
   * Creates the interpreter for Postgres
   * @param command_factory to convert packet into commands
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
  Transition Process(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                     common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                     common::ManagedPointer<ConnectionContext> context, NetworkCallback callback) override;

  // TODO(Tianyu): Fill in the following documentation at some point
  /**
   *
   * @param out
   */
  void GetResult(const common::ManagedPointer<WriteQueue> out) override {
    PostgresPacketWriter writer(out);
    ExecQueryMessageGetResult(&writer, ResultType::SUCCESS);
  }

  /**
   *
   * @param in
   * @param out
   * @return
   */
  Transition ProcessStartup(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out);

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

 protected:
  /**
   * @see ProtocolInterpreter::GetPacketHeaderSize
   * Header format: 1 byte message type (only if non-startup)
   *              + 4 byte message size (inclusive of these 4 bytes)
   */
  size_t GetPacketHeaderSize() override;

  /**
   * @see ProtocolInterpreter::SetPacketMessageType
   */
  void SetPacketMessageType(common::ManagedPointer<ReadBuffer> in) override;

 private:
  bool startup_ = true;
  std::unordered_map<std::string, std::string> cmdline_options_;
  common::ManagedPointer<PostgresCommandFactory> command_factory_;
};

}  // namespace terrier::network
