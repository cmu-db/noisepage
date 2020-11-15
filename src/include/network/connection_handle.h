#pragma once

#include <memory>
#include <utility>

#include "common/managed_pointer.h"
#include "network/connection_context.h"
#include "network/network_types.h"

struct event;

namespace noisepage::network {

class ConnectionHandlerTask;
class NetworkIoWrapper;
class ProtocolInterpreter;

/**
 * ConnectionHandle encapsulates IO-related information throughout the lifetime of a client connection.
 *
 * @warning Don't add state to this class unless it involves the state machine. Instead, consider ConnectionContext.
 */
class ConnectionHandle {
 public:
  /**
   * @brief Construct a new ConnectionHandle.
   *
   * @param sock_fd Client connection's file descriptor.
   * @param task The task responsible for this handle's creation.
   * @param tcop The traffic cop to be used.
   * @param interpreter protocol Interpreter to use for this connection handle.
   */
  ConnectionHandle(int sock_fd, common::ManagedPointer<ConnectionHandlerTask> task,
                   common::ManagedPointer<trafficcop::TrafficCop> tcop,
                   std::unique_ptr<ProtocolInterpreter> interpreter);

  /** Reset this connection handle. */
  ~ConnectionHandle();

  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(ConnectionHandle);

  /**
   * @brief Signal that this ConnectionHandle is ready to handle events.
   *
   * This method needs to be called separately after initialization for the
   * connection handle to do anything. The reason why this is not performed in
   * the constructor is because it publishes pointers to this object. While the
   * object should be fully initialized at that point, it's never a bad idea
   * to be careful.
   *
   * TODO(WAN): Check the design of this API. What needs to happen in between object construction and calling this
   *   function? If nothing can be done, perhaps this logic should be moved to the constructor.
   */
  void RegisterToReceiveEvents();

  /**
   * Handles a libevent event. This simply delegates to the state machine.
   */
  void HandleEvent(int fd, int16_t flags);

  /**
   * @brief Tries to read from the event port onto the read buffer
   * @return The transition to trigger in the state machine after
   */
  Transition TryRead();

  /**
   * @brief Flushes the write buffer to the client if needed
   * @return The transition to trigger in the state machine after
   */
  Transition TryWrite();

  /**
   * @brief Processes the client's input that has been fed into the ReadBuffer
   * @return The transition to trigger in the state machine after
   */
  Transition Process();

  /**
   * @brief Gets a computed result that the client requested
   * @return The transition to trigger in the state machine after
   */
  Transition GetResult();

  /**
   * @brief Tries to close the current client connection
   * @return The transition to trigger in the state machine after
   */
  Transition TryCloseConnection();

  /**
   * Updates the event flags of the network event. This configures how the
   * handler reacts to client activity from this connection.
   * @param flags new flags for the event handle.
   * @param timeout_secs number of seconds for timeout for this event, this is ignored if flags doesn't include
   * EV_TIMEOUT
   */
  void UpdateEventFlags(int16_t flags, int timeout_secs = 0);

  /**
   * Stops receiving network events from client connection. This is useful when
   * we are waiting on noisepage to return the result of a query and not handling
   * client query.
   */
  void StopReceivingNetworkEvent();

  /**
   * issues a libevent to wake up the state machine in the WAIT_ON_NOISEPAGE state
   * @param callback_args this for a ConnectionHandle in WAIT_ON_NOISEPAGE state
   */
  static void Callback(void *callback_args);

 private:
  /** Reset the state of this connection handle for reuse. This should only be called by ConnectionHandleFactory. */
  void ResetForReuse(connection_id_t connection_id, common::ManagedPointer<ConnectionHandlerTask> task,
                     std::unique_ptr<ProtocolInterpreter> interpreter);

  /**
   * A state machine is defined to be a set of states, a set of symbols it
   * supports, and a function mapping each
   * state and symbol pair to the state it should transition to. i.e.
   * transition_graph = state * symbol -> state
   *
   * In addition to the transition system, our network state machine also needs
   * to perform actions. Actions are
   * defined as functions (lambdas, or closures, in various other languages) and
   * is promised to be invoked by the
   * state machine after each transition if registered in the transition graph.
   *
   * So the transition graph overall has type transition_graph = state * symbol
   * -> state * action
   */
  class StateMachine {
   public:
    using Action = Transition (*)(const common::ManagedPointer<ConnectionHandle>);
    using TransitionResult = std::pair<ConnState, Action>;

    /**
     * @brief Run the state machine on @p action until a halting state is reached.
     *
     * @param action The starting action to be taken by the state machine.
     * @param handle The connection handle that state machine actions should be applied to.
     */
    void Accept(Transition action, common::ManagedPointer<ConnectionHandle> handle);

    /** @return The current state of the state machine. */
    ConnState CurrentState() const { return current_state_; }

   private:
    /**
     * Delta is a standard state machine transition function (ConnState x Transition -> TransitionResult).
     * Equivalently, (Old state x Transition -> New state x Action to be taken).
     */
    static TransitionResult Delta(ConnState state, Transition transition);

    /** The StateMachine starts in the READ state. */
    ConnState current_state_ = ConnState::READ;
  };

  friend class ConnectionHandleFactory;
  friend class ConnectionHandleStateMachineTransition;
  friend class StateMachine;

  // See class header warning; don't add state here unless required for the StateMachine.

  std::unique_ptr<NetworkIoWrapper> io_wrapper_;
  common::ManagedPointer<ConnectionHandlerTask> conn_handler_task_;
  common::ManagedPointer<trafficcop::TrafficCop> traffic_cop_;
  std::unique_ptr<ProtocolInterpreter> protocol_interpreter_;

  StateMachine state_machine_{};
  struct event *network_event_ = nullptr;
  struct event *workpool_event_ = nullptr;

  ConnectionContext context_;
};
}  // namespace noisepage::network
