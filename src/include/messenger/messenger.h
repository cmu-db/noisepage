#pragma once

#include <string>
#include <zmq.hpp>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"
#include "messenger/messenger_logic.h"

namespace terrier::messenger {

class ConnectionId;
class MessengerLogic;

class ConnectionDestination {
 public:
  /** @return A TCP destination in ZMQ format. */
  static ConnectionDestination MakeTCP(std::string_view hostname, int port);
  /** @return An IPC destination in ZMQ format. Pathname must be a valid filesystem path, e.g., /tmp/noisepage/ipc/0. */
  static ConnectionDestination MakeIPC(std::string_view pathname);
  /** @return An in-process destination in ZMQ format. */
  static ConnectionDestination MakeInProc(std::string_view endpoint);

  /** @return The destination in ZMQ format. */
  const char *GetDestination() const { return zmq_address_; }

 private:
  /** Construct a new ConnectionDestination with the specified address. */
  explicit ConnectionDestination(const char *zmq_address) : zmq_address_(zmq_address) {}
  const char *zmq_address_;
};

/**
 *
 * @see messenger.cpp for a crash course on ZeroMQ, the current backing implementation.
 */
class Messenger : public common::DedicatedThreadTask {
 public:
  explicit Messenger(common::ManagedPointer<MessengerLogic> messenger_logic);

  void RunTask() override;

  void Terminate() override;

  /**
   * @param target      The destination to be connected to.
   * @param identity    An optional string to identify the connection by. See warning!
   * @return            A new ConnectionId. See warning!
   *
   * @warning           Identities must be unique to the Messenger instance that you are connecting to!
   * @warning           DO NOT USE THIS ConnectionId FROM A DIFFERENT THREAD THAN THE CALLER OF THIS FUNCTION!
   *                    Make a new connection instead, connections are cheap.
   */
  ConnectionId MakeConnection(const ConnectionDestination &target, std::optional<std::string> identity);

  void SendMessage(ConnectionId *connection_id, std::string message);

 private:
  static constexpr int MESSENGER_PORT = 9022;
  static constexpr const char *MESSENGER_DEFAULT_TCP = "tcp://*:9022";
  static constexpr const char *MESSENGER_DEFAULT_IPC = "ipc:///tmp/noisepage-ipc0";
  static constexpr const char *MESSENGER_DEFAULT_INPROC = "inproc://noisepage-inproc";
  static constexpr const char *MESSENGER_INTERNAL_INPROC = "inproc:://messenger-internal-inproc";

  /** The main server loop. */
  void ServerLoop();

  common::ManagedPointer<MessengerLogic> messenger_logic_;
  zmq::context_t zmq_ctx_;
  zmq::socket_t zmq_default_socket_;
  bool messenger_running_ = false;
};

class MessengerOwner : public common::DedicatedThreadOwner {
 public:
  explicit MessengerOwner(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry);

 private:
  MessengerLogic logic_;
  common::ManagedPointer<Messenger> messenger_;
};

}  // namespace terrier::messenger
