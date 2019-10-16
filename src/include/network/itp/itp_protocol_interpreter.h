#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/connection_handle.h"
#include "network/postgres/itp_command_factory.h"
#include "network/postgres/itp_network_commands.h"
#include "network/postgres/itp_packet_writer.h"
#include "network/protocol_interpreter.h"

namespace terrier::network {

/**
 * Interprets the network protocol for postgres clients
 */
class ITPProtocolInterpreter : public ProtocolInterpreter {
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
    explicit Provider(common::ManagedPointer<ITPCommandFactory> command_factory) : command_factory_(command_factory) {}

    /**
     * @return an instance of the protocol interpreter
     */
    std::unique_ptr<ProtocolInterpreter> Get() override {
      return std::make_unique<ITPProtocolInterpreter>(command_factory_);
    }

   private:
    common::ManagedPointer<ITPCommandFactory> command_factory_;
  };

  /**
   * Default constructor
   */
  explicit ITPProtocolInterpreter(common::ManagedPointer<PostgresCommandFactory> command_factory)
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
                     common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                     common::ManagedPointer<ConnectionContext> context, NetworkCallback callback) override;

  /**
   * Writes result to the client
   * @param out
   */
  void GetResult(std::shared_ptr<WriteQueue> out) override {
    ITPPacketWriter writer(out);
    out->BeginPacket(NetworkMessageType::ITP_COMMAND_COMPLETE).EndPacket();
  }

 private:
  bool startup_ = true;
  InputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;
  common::ManagedPointer<ITPCommandFactory> command_factory_;

  bool TryBuildPacket(const std::shared_ptr<ReadBuffer> &in);
  bool TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in);
};

}  // namespace terrier::network
