#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "loggers/network_logger.h"
#include "network/connection_context.h"
#include "network/connection_handle.h"
#include "network/itp/itp_command_factory.h"
#include "network/itp/itp_network_commands.h"
#include "network/itp/itp_packet_writer.h"
#include "network/protocol_interpreter.h"

namespace noisepage::network {

/**
 * Interprets the network protocol for ITP
 */
class ITPProtocolInterpreter : public ProtocolInterpreter {
 public:
  /**
   * The provider encapsulates the creation logic of a protocol interpreter into an object
   */
  struct Provider : public ProtocolInterpreterProvider {
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
   * Creates the interpreter for ITP
   * @param command_factory to convert packet into commands
   */
  explicit ITPProtocolInterpreter(common::ManagedPointer<ITPCommandFactory> command_factory)
      : command_factory_(command_factory) {}

  /**
   * @see ProtocolIntepreter::Process
   * @param in The ReadBuffer to read input from
   * @param out The WriteQueue to communicate with the client through
   * @param t_cop the traffic cop pointer
   * @param context the connection context
   * @return
   */
  Transition Process(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                     common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                     common::ManagedPointer<ConnectionContext> context) override;

  void Teardown(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                common::ManagedPointer<ConnectionContext> context) override {}

  /**
   * Writes result to the client
   * @param out WriteQueue to flush message to client
   */
  void GetResult(common::ManagedPointer<WriteQueue> out) override;

 protected:
  /**
   * @see ProtocolInterpreter::GetPacketHeaderSize
   * Header format: 1 byte message type + 4 byte message size
   */
  size_t GetPacketHeaderSize() override;

  /**
   * @see ProtocolInterpreter::SetPacketMessageType
   * @param in ReadBuffer to read input from
   */
  void SetPacketMessageType(common::ManagedPointer<ReadBuffer> in) override;

 private:
  common::ManagedPointer<ITPCommandFactory> command_factory_;
};

}  // namespace noisepage::network
