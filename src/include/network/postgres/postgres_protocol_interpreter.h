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

constexpr uint32_t INITIAL_BACKOFF_TIME = 2;
constexpr uint32_t BACKOFF_FACTOR = 2;
constexpr uint32_t MAX_BACKOFF_TIME = 20;

/**
 * Interprets the network protocol for postgres clients. Any state/logic that is Postgres protocol-specific should live
 * at this layer.
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
   * @param in buffer to read packets from
   * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
   * @param t_cop non-owning pointer to the traffic cop to pass down to the command layer
   * @param context connection-specific (not protocol) state
   * @return next transition for ConnectionHandle's state machine
   */
  Transition Process(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                     common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                     common::ManagedPointer<ConnectionContext> context) override;

  /**
   * Closes any protocol-specific state. We currently use this to remove the temporary namespace for Postgres.
   * @param in buffer to read packets from
   * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
   * @param t_cop non-owning pointer to the traffic cop to pass down to the command layer
   * @param context connection-specific (not protocol) state
   */
  void Teardown(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                common::ManagedPointer<ConnectionContext> context) override;

  // TODO(Tianyu): Fill in the following documentation at some point
  /**
   * (Matt): I think this was used in Peloton for asynchronous execution, after a callback wakeup the ConnectionHandle
   * would call to the protocol interpreter to get the state of the asynchronous query after the output was already
   * complete.
   * @param out
   */
  void GetResult(const common::ManagedPointer<WriteQueue> out) override {}

  /**
   * Handles all of the set up for the Postgres protocol. Currently that includes saying no to SSL support during
   * handshake, checking protocol version, parsing client arguments, and creating the temporary namespace for this
   * connection.
   * @param in buffer to read packets from
   * @param out buffer to send results back out on (doesn't really happen if TERMINATE is returned)
   * @param t_cop non-owning pointer to the traffic cop for use in any cleanup necessary
   * @param context where to stash connection-specific state
   * @return transition::PROCEED if it succeeded, transition::TERMINATE otherwise
   */
  Transition ProcessStartup(common::ManagedPointer<ReadBuffer> in, common::ManagedPointer<WriteQueue> out,
                            common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                            common::ManagedPointer<ConnectionContext> context);

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
  common::ManagedPointer<PostgresCommandFactory> command_factory_;
};

}  // namespace terrier::network
