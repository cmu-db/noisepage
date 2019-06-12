#pragma once
#include <functional>
#include <memory>
#include "common/managed_pointer.h"
#include "network/connection_context.h"
#include "network/network_io_utils.h"
#include "network/network_types.h"
//
namespace terrier::network {

class ConnectionHandle;

/**
 * Interface to communicate with a client via a certain network protocol
 */
class ProtocolInterpreter {
 public:
  /**
   * A Provider interface is a strategy object for construction.
   *
   * It encapsulates creation logic that can be passed around as polymorphic objects. Inject the
   * approriate subclass of this object to the connection dispatcher in order to bind them to
   * the correct protocol type.
   */
  struct Provider {
    virtual ~Provider() = default;
    /**
     * @return a constructed instance of protocol interpreter
     */
    virtual std::unique_ptr<ProtocolInterpreter> Get() = 0;
  };
  /**
   * Processes client's input that has been fed into the given ReadBufer
   * @param in The ReadBuffer to read input from
   * @param out The WriteQueue to communicate with the client through
   * @param t_cop The traffic cop pointer
   * @param context the connection context
   * @param callback The callback function to trigger on completion
   * @return The next transition for the client's associated state machine
   */
  virtual Transition Process(std::shared_ptr<ReadBuffer> in, std::shared_ptr<WriteQueue> out,
                             common::ManagedPointer<tcop::TrafficCop> t_cop,
                             common::ManagedPointer<ConnectionContext> context,
                             NetworkCallback callback) = 0;

  /**
   * Sends a result
   * @param out
   */
  virtual void GetResult(std::shared_ptr<WriteQueue> out) = 0;

  /**
   * Default destructor for ProtocolInterpreter
   */
  virtual ~ProtocolInterpreter() = default;
};
//
}  // namespace terrier::network
