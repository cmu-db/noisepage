#include <unistd.h>
#include <cstring>
#include <memory>
#include <utility>

#include "network/connection_dispatcher_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"

#include "common/utility.h"

/*
 *  Here we are abusing macro expansion to allow for human readable definition
 *  of the finite state machine's transition table.
 *
 *  The macros merely compose a large function. Unless you know what you are
 *  doing, do not modify their definitions.
 *
 *  To use these macros, follow the examples given. Or, here is a syntax chart:
 *
 *  x list ::= x \n (x list) (each item of x separated by new lines)
 *  graph ::= DEF_TRANSITION_GRAPH
 *              state list
 *            END_DEF
 *
 *  state ::= DEFINE_STATE(ConnState)
 *             transition list
 *            END_STATE_DEF
 *
 *  transition ::=
 *  ON (Transition) SET_STATE_TO (ConnState) AND_INVOKE (ConnectionHandle
 * method)
 *
 *  Note that all the symbols used must be defined in ConnState, Transition and
 *  ClientSocketWrapper, respectively.
 *
 */

// clang-format off
// Underneath the hood these macro is defining the static method
// ConnectionHandle::StateMachine::Delta.
// Together they compose a nested switch statement. Running the function on any
// undefined state or transition on said state will throw a runtime error.

#define DEF_TRANSITION_GRAPH                                                                                        \
  ConnectionHandle::StateMachine::transition_result ConnectionHandle::StateMachine::Delta(ConnState state,         \
                                                                                           Transition transition) { \
    switch (state) {
#define DEFINE_STATE(s) \
  case ConnState::s: {  \
    switch (transition) {
#define ON(t)         \
  case Transition::t: \
    return
#define SET_STATE_TO(s) \
  {                     \
    ConnState::s,
#define AND_INVOKE(m)                         \
  ([](ConnectionHandle &w) { return w.m(); }) \
  };                                          // NOLINT

#define AND_WAIT_ON_READ                      \
  ([](ConnectionHandle &w) {                  \
    w.UpdateEventFlags(EV_READ | EV_PERSIST); \
    return Transition::NONE;                  \
  })                                          \
  };                                          // NOLINT

#define AND_WAIT_ON_WRITE                      \
  ([](ConnectionHandle &w) {                   \
    w.UpdateEventFlags(EV_WRITE | EV_PERSIST); \
    return Transition::NONE;                   \
  })                                           \
  };                                            // NOLINT

#define AND_WAIT_ON_TERRIER        \
  ([](ConnectionHandle &w) {       \
    w.StopReceivingNetworkEvent(); \
    return Transition::NONE;       \
  })                               \
  };                               // NOLINT

#define AND_WAIT_ON_READ_TIMEOUT        \
  ([](ConnectionHandle &w) {       \
    w.UpdateEventFlags(EV_READ | EV_PERSIST | EV_TIMEOUT, READ_TIMEOUT); \
    return Transition::NONE;       \
  })                               \
  };                               // NOLINT

#define END_DEF                                       \
  default:                                            \
    throw std::runtime_error("undefined transition"); \
    }                                                 \
    }                                                  // NOLINT

#define END_STATE_DEF ON(TERMINATE) SET_STATE_TO(CLOSING) AND_INVOKE(TryCloseConnection) END_DEF

namespace terrier::network {

DEF_TRANSITION_GRAPH
    DEFINE_STATE(READ)
        ON(WAKEUP) SET_STATE_TO(READ) AND_INVOKE(TryRead)
        ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
        ON(NEED_READ) SET_STATE_TO(READ) AND_WAIT_ON_READ
        ON(NEED_READ_TIMEOUT) SET_STATE_TO(READ) AND_WAIT_ON_READ_TIMEOUT
        // This case happens only when we use SSL and are blocked on a write
        // during handshake. From terrier's perspective we are still waiting
        // for reads.
        ON(NEED_WRITE) SET_STATE_TO(READ) AND_WAIT_ON_WRITE
    END_STATE_DEF

// DEFINE_STATE(SSL_INIT)
//     ON(WAKEUP) SET_STATE_TO(SSL_INIT) AND_INVOKE(TrySslHandshake)
//     ON(NEED_READ) SET_STATE_TO(SSL_INIT) AND_WAIT_ON_READ
//     ON(NEED_WRITE) SET_STATE_TO(SSL_INIT) AND_WAIT_ON_WRITE
//     ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
// END_STATE_DEF

    DEFINE_STATE(PROCESS)
        ON(WAKEUP) SET_STATE_TO(PROCESS) AND_INVOKE(GetResult)
        ON(PROCEED) SET_STATE_TO(WRITE) AND_INVOKE(TryWrite)
        ON(NEED_READ) SET_STATE_TO(READ) AND_INVOKE(TryRead)
        ON(NEED_READ_TIMEOUT) SET_STATE_TO(READ) AND_WAIT_ON_READ_TIMEOUT
        // Client connections are ignored while we wait on terrier
        // to execute the query
        ON(NEED_RESULT) SET_STATE_TO(PROCESS) AND_WAIT_ON_TERRIER
        // ON(NEED_SSL_HANDSHAKE) SET_STATE_TO(SSL_INIT) AND_INVOKE(TrySslHandshake)
    END_STATE_DEF

    DEFINE_STATE(WRITE)
        ON(WAKEUP) SET_STATE_TO(WRITE) AND_INVOKE(TryWrite)
        // This happens when doing ssl-rehandshake with client
        ON(NEED_READ) SET_STATE_TO(WRITE) AND_WAIT_ON_READ
        ON(NEED_WRITE) SET_STATE_TO(WRITE) AND_WAIT_ON_WRITE
        ON(PROCEED) SET_STATE_TO(PROCESS) AND_INVOKE(Process)
    END_STATE_DEF

    DEFINE_STATE(CLOSING)
        ON(WAKEUP) SET_STATE_TO(CLOSING) AND_INVOKE(TryCloseConnection)
        ON(NEED_READ) SET_STATE_TO(WRITE) AND_WAIT_ON_READ
        ON(NEED_WRITE) SET_STATE_TO(WRITE) AND_WAIT_ON_WRITE
    END_STATE_DEF
END_DEF
    // clang-format on

    void ConnectionHandle::StateMachine::Accept(Transition action, ConnectionHandle &connection) {
  Transition next = action;
  while (next != Transition::NONE) {
    transition_result result = Delta(current_state_, next);
    current_state_ = result.first;
    try {
      next = result.second(connection);
    } catch (NetworkProcessException &e) {
      NETWORK_LOG_ERROR("{0}\n", e.what());
      next = Transition::TERMINATE;
    }
  }
}

Transition ConnectionHandle::GetResult() {
  EventUtil::EventAdd(network_event_, nullptr);
  protocol_interpreter_->GetResult(io_wrapper_->GetWriteQueue());
  NETWORK_LOG_TRACE("GetResult");
  return Transition::PROCEED;
}

Transition ConnectionHandle::TryCloseConnection() {
  NETWORK_LOG_TRACE("Attempt to close the connection {0}", io_wrapper_->GetSocketFd());
  Transition close = io_wrapper_->Close();
  if (close != Transition::PROCEED) return close;
  // Remove listening event
  // Only after the connection is closed is it safe to remove events,
  // after this point no object in the system has reference to this
  // connection handle and we will need to destruct and exit.
  conn_handler_->UnregisterEvent(network_event_);
  conn_handler_->UnregisterEvent(workpool_event_);

  return Transition::NONE;
}

}  // namespace terrier::network
