#pragma once

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <unordered_map>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/managed_pointer.h"
#include "loggers/network_logger.h"

#include "network/connection_context.h"
#include "network/connection_handler_task.h"
#include "network/network_io_wrapper.h"
#include "network/network_types.h"
#include "network/postgres/postgres_command_factory.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/protocol_interpreter.h"

#include "traffic_cop/traffic_cop.h"
namespace terrier::network {

/**
 * A ConnectionHandle encapsulates all information we need to do IO about
 * a client connection for its entire duration. This includes a state machine
 * and the necessary libevent infrastructure for a handler to work on this
 * connection.
 */
class ConnectionHandle {
 public:
  /**
   * Constructs a new ConnectionHandle
   * @param sock_fd Client's connection fd
   * @param handler The handler responsible for this handle
   * @param tcop The pointer to the traffic cop
   * @param interpreter protocol interpreter to use for this connection handle
   */
  ConnectionHandle(int sock_fd, common::ManagedPointer<ConnectionHandlerTask> handler,
                   common::ManagedPointer<trafficcop::TrafficCop> tcop,
                   std::unique_ptr<ProtocolInterpreter> interpreter)
      : io_wrapper_(std::make_unique<NetworkIoWrapper>(sock_fd)),
        conn_handler_(handler),
        traffic_cop_(tcop),
        protocol_interpreter_(std::move(interpreter)) {}

  ~ConnectionHandle() { context_.Reset(); }

  /**
   * Disable copying and moving ConnectionHandle instances
   */
  DISALLOW_COPY_AND_MOVE(ConnectionHandle);

  /**
   * @brief Signal to libevent that this ConnectionHandle is ready to handle
   * events
   *
   * This method needs to be called separately after initialization for the
   * connection handle to do anything. The reason why this is not performed in
   * the constructor is because it publishes pointers to this object. While the
   * object should be fully initialized at that point, it's never a bad idea
   * to be careful.
   */
  void RegisterToReceiveEvents() {
    workpool_event_ = conn_handler_->RegisterManualEvent(METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);

    network_event_ = conn_handler_->RegisterEvent(io_wrapper_->GetSocketFd(), EV_READ | EV_PERSIST,
                                                  METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
  }

  /**
   * Handles a libevent event. This simply delegates the the state machine.
   */
  void HandleEvent(int fd, int16_t flags) {
    Transition t;
    if ((flags & EV_TIMEOUT) != 0) {
      t = Transition::TERMINATE;
      NETWORK_LOG_TRACE("Timeout occurred on file descriptor {0}", fd);
    } else {
      t = Transition ::WAKEUP;
    }
    state_machine_.Accept(t, *this);
  }

  /* State Machine Actions */
  /**
   * @brief Tries to read from the event port onto the read buffer
   * @return The transition to trigger in the state machine after
   */
  Transition TryRead() { return io_wrapper_->FillReadBuffer(); }

  /**
   * @brief Flushes the write buffer to the client if needed
   * @return The transition to trigger in the state machine after
   */
  Transition TryWrite() {
    if (io_wrapper_->ShouldFlush()) return io_wrapper_->FlushAllWrites();

    return Transition::PROCEED;
  }

  /**
   * @brief Processes the client's input that has been fed into the ReadBuffer
   * @return The transition to trigger in the state machine after
   */
  Transition Process() {
    return protocol_interpreter_->Process(io_wrapper_->GetReadBuffer(), io_wrapper_->GetWriteQueue(), traffic_cop_,
                                          common::ManagedPointer(&context_),
                                          [=] { event_active(workpool_event_, EV_WRITE, 0); });
  }

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
  void UpdateEventFlags(int16_t flags, int timeout_secs = 0) {
    if ((flags & EV_TIMEOUT) != 0) {
      struct timeval timeout;
      struct timeval *timeout_str;
      timeout_str = &timeout;
      timeout.tv_usec = 0;
      timeout.tv_sec = timeout_secs;
      conn_handler_->UpdateEvent(network_event_, io_wrapper_->GetSocketFd(), flags,
                                 METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this, timeout_str);
    } else {
      conn_handler_->UpdateEvent(network_event_, io_wrapper_->GetSocketFd(), flags,
                                 METHOD_AS_CALLBACK(ConnectionHandle, HandleEvent), this);
    }
  }

  /**
   * Stops receiving network events from client connection. This is useful when
   * we are waiting on terrier to return the result of a query and not handling
   * client query.
   */
  void StopReceivingNetworkEvent() { EventUtil::EventDel(network_event_); }

 private:
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
    using action = Transition (*)(ConnectionHandle &);
    using transition_result = std::pair<ConnState, action>;
    /**
     * Runs the internal state machine, starting from the symbol given, until no
     * more
     * symbols are available.
     *
     * Each state of the state machine defines a map from a transition symbol to
     * an action
     * and the next state it should go to. The actions can either generate the
     * next symbol,
     * which means the state machine will continue to run on the generated
     * symbol, or signal
     * that there is no more symbols that can be generated, at which point the
     * state machine
     * will stop running and return, waiting for an external event (user
     * interaction, or system event)
     * to generate the next symbol.
     *
     * @param action starting symbol
     * @param connection the network connection object to apply actions to
     */
    void Accept(Transition action, ConnectionHandle &connection);  // NOLINT
    // clang-tidy is suppressed here as it complains about the reference param having
    // no const qualifier as this must be casted into a (void*) to pass as an argument to METHOD_AS_CALLBACK
    // in the forward dependencies of Accept in the state_machine

   private:
    /**
     * delta is the transition function that defines, for each state, its
     * behavior and the
     * next state it should go to.
     */
    static transition_result Delta(ConnState state, Transition transition);
    ConnState current_state_ = ConnState::READ;
  };

  friend class StateMachine;
  friend class ConnectionHandleFactory;

  std::unique_ptr<NetworkIoWrapper> io_wrapper_;
  // A raw pointer is used here because references cannot be rebound.
  common::ManagedPointer<ConnectionHandlerTask> conn_handler_;
  common::ManagedPointer<trafficcop::TrafficCop> traffic_cop_;
  std::unique_ptr<ProtocolInterpreter> protocol_interpreter_;

  StateMachine state_machine_{};
  struct event *network_event_ = nullptr, *workpool_event_ = nullptr;

  // TODO(Tianyu): Do we want to flatten this struct out into connection handle, or is this current separation
  // sensible?
  ConnectionContext context_;
};
}  // namespace terrier::network
