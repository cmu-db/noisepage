#include "network/connection_handle.h"

#include <memory>

#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"

namespace terrier::network {

class ConnectionHandleHelper {
 public:
  // clang-format off
  /** Implement transition for ConnState::READ. */
  static ConnectionHandle::StateMachine::transition_result TransitionForRead(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::READ, WaitForRead};
      case Transition::NEED_READ_TIMEOUT:   return {ConnState::READ, WaitForReadWithTimeout};
        // Allegedly, the NEED_WRITE case happens only when we use SSL and are blocked on a write
        // during handshake. From terrier's perspective we are still waiting
        // for reads.
      case Transition::NEED_WRITE:          return {ConnState::READ, WaitForWrite};
      case Transition::PROCEED:             return {ConnState::PROCESS, Process};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::READ, TryRead};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::PROCESS. */
  static ConnectionHandle::StateMachine::transition_result TransitionForProcess(Transition transition) {
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
  static ConnectionHandle::StateMachine::transition_result TransitionForWrite(Transition transition) {
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
  static ConnectionHandle::StateMachine::transition_result TransitionForClosing(Transition transition) {
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

  static Transition WaitForRead(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST);
    return Transition::NONE;
  }

  static Transition WaitForWrite(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_WRITE | EV_PERSIST);
    return Transition::NONE;
  }

  static Transition WaitForReadWithTimeout(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST | EV_TIMEOUT, READ_TIMEOUT);
    return Transition::NONE;
  }

  static Transition WaitForTerrier(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->StopReceivingNetworkEvent();
    return Transition::NONE;
  }
};

ConnectionHandle::StateMachine::transition_result ConnectionHandle::StateMachine::Delta(ConnState state,
                                                                                        Transition transition) {
  switch (state) {
    case ConnState::READ:
      return ConnectionHandleHelper::TransitionForRead(transition);
    case ConnState::PROCESS:
      return ConnectionHandleHelper::TransitionForProcess(transition);
    case ConnState::WRITE:
      return ConnectionHandleHelper::TransitionForWrite(transition);
    case ConnState::CLOSING:
      return ConnectionHandleHelper::TransitionForClosing(transition);
    default: {
      throw std::runtime_error("Undefined transition!");
    }
  }
}
void ConnectionHandle::StateMachine::Accept(Transition action,
                                            const common::ManagedPointer<ConnectionHandle> connection) {
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

  protocol_interpreter_->Teardown(io_wrapper_->GetReadBuffer(), io_wrapper_->GetWriteQueue(), traffic_cop_,
                                  common::ManagedPointer(&context_));

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
