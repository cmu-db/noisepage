#include "network/connection_handle.h"

#include <memory>

#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

/** ConnectionHandleStateMachineTransition implements ConnectionHandle::StateMachine::Delta's transition function. */
class ConnectionHandleStateMachineTransition {
 public:
  // clang-format off
  /** Implement transition for ConnState::READ. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForRead(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::READ, WaitForRead};
      case Transition::NEED_READ_TIMEOUT:   return {ConnState::READ, WaitForReadWithTimeout};
      // Allegedly, the NEED_WRITE case happens only when we use SSL and are blocked on a write
      // during handshake. From terrier's perspective we are still waiting for reads.
      case Transition::NEED_WRITE:          return {ConnState::READ, WaitForWrite};
      case Transition::PROCEED:             return {ConnState::PROCESS, Process};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::READ, TryRead};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::PROCESS. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForProcess(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::READ, TryRead};
      case Transition::NEED_READ_TIMEOUT:   return {ConnState::READ, WaitForReadWithTimeout};
      case Transition::NEED_RESULT:         return {ConnState::PROCESS, WaitForTerrier};
      case Transition::PROCEED:             return {ConnState::WRITE, TryWrite};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::PROCESS, GetResult};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::WRITE. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForWrite(Transition transition) {
    switch (transition) {
        // Allegedly, NEED_READ happens during ssl-rehandshake with the client.
      case Transition::NEED_READ:           return {ConnState::WRITE, WaitForRead};
      case Transition::NEED_WRITE:          return {ConnState::WRITE, WaitForWrite};
      case Transition::PROCEED:             return {ConnState::PROCESS, Process};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::WRITE, TryWrite};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::CLOSING. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForClosing(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::WRITE, WaitForRead};
      case Transition::NEED_WRITE:          return {ConnState::WRITE, WaitForWrite};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::CLOSING, TryCloseConnection};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }
  // clang-format on

 private:
  /** Define a function (ManagedPointer<ConnectionHandle> -> Transition) that calls the underlying handle's function. */
#define DEF_HANDLE_WRAPPER_FN(function_name)                                               \
  static Transition function_name(const common::ManagedPointer<ConnectionHandle> handle) { \
    return handle->function_name();                                                        \
  }

  DEF_HANDLE_WRAPPER_FN(GetResult);
  DEF_HANDLE_WRAPPER_FN(Process);
  DEF_HANDLE_WRAPPER_FN(TryRead);
  DEF_HANDLE_WRAPPER_FN(TryWrite);
  DEF_HANDLE_WRAPPER_FN(TryCloseConnection);

#undef DEF_HANDLE_WRAPPER_FN

  /** Wait for the connection to become readable. */
  static Transition WaitForRead(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST);
    return Transition::NONE;
  }

  /** Wait for the connection to become writable. */
  static Transition WaitForWrite(const common::ManagedPointer<ConnectionHandle> handle) {
    // Wait for the connection to become writable.
    handle->UpdateEventFlags(EV_WRITE | EV_PERSIST);
    return Transition::NONE;
  }

  /** Wait for the connection to become readable, or until a timeout happens. */
  static Transition WaitForReadWithTimeout(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST | EV_TIMEOUT, READ_TIMEOUT);
    return Transition::NONE;
  }

  /** Stop listening to network events. This is used when control is completely ceded to Terrier, hence the name. */
  static Transition WaitForTerrier(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->StopReceivingNetworkEvent();
    return Transition::NONE;
  }
};

ConnectionHandle::StateMachine::TransitionResult ConnectionHandle::StateMachine::Delta(ConnState state,
                                                                                        Transition transition) {
  // clang-format off
  switch (state) {
    case ConnState::READ:      return ConnectionHandleStateMachineTransition::TransitionForRead(transition);
    case ConnState::PROCESS:   return ConnectionHandleStateMachineTransition::TransitionForProcess(transition);
    case ConnState::WRITE:     return ConnectionHandleStateMachineTransition::TransitionForWrite(transition);
    case ConnState::CLOSING:   return ConnectionHandleStateMachineTransition::TransitionForClosing(transition);
    default:                   throw std::runtime_error("Undefined transition!");
  }
  // clang-format on
}

void ConnectionHandle::StateMachine::Accept(Transition action,
                                            const common::ManagedPointer<ConnectionHandle> connection) {
  Transition next = action;
  // Transition until there are no more transitions.
  while (next != Transition::NONE) {
    TransitionResult result = Delta(current_state_, next);
    current_state_ = result.first;
    try {
      next = result.second(connection);
    } catch (const NetworkProcessException &e) {
      // If an error occurs, the error is logged and the connection is terminated.
      NETWORK_LOG_ERROR("{0}\n", e.what());
      next = Transition::TERMINATE;
    }
  }
}

Transition ConnectionHandle::GetResult() {
  // Wait until a network event happens.
  EventUtil::EventAdd(network_event_, EventUtil::EVENT_ADD_WAIT_FOREVER);
  // TODO(WAN): It is absolutely not clear to me wtf is this doing. Nothing?
  protocol_interpreter_->GetResult(io_wrapper_->GetWriteQueue());
  return Transition::PROCEED;
}

Transition ConnectionHandle::TryCloseConnection() {
  protocol_interpreter_->Teardown(io_wrapper_->GetReadBuffer(), io_wrapper_->GetWriteQueue(), traffic_cop_,
                                  common::ManagedPointer(&context_));

  Transition close = io_wrapper_->Close();
  if (close != Transition::PROCEED) return close;
  // Remove listening event
  // Only after the connection is closed is it safe to remove events,
  // after this point no object in the system has reference to this
  // connection handle and we will need to destruct and exit.

  // The connection must be closed.
  conn_handler_->UnregisterEvent(network_event_);
  conn_handler_->UnregisterEvent(workpool_event_);

  return Transition::NONE;
}

}  // namespace terrier::network
