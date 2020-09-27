#pragma once

#include <memory>
#include <optional>
#include <string>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"
#include "messenger/messenger_logic.h"

// All zmq objects are forward-declared and allocated on the heap.
// This is to avoid leaking zmq headers into the rest of the system.
// The prediction is that doing this won't hurt performance too much.
namespace zmq {
class context_t;
class socket_t;
}  // namespace zmq

namespace terrier::messenger {

class ConnectionDestination;

/** ConnectionId is an abstraction around establishing connections. */
class ConnectionId {
 public:
  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~ConnectionId();

 private:
  friend Messenger;
  /**
   * Create a new ConnectionId that is connected to the specified target.
   * @param zmq_ctx     The ZeroMQ context that holds this connection.
   * @param target      The target to be connected to.
   * @param identity    The name that the connection should have.
   */
  explicit ConnectionId(common::ManagedPointer<zmq::context_t> zmq_ctx, const ConnectionDestination &target,
                        std::string_view identity);

  /** The ZMQ socket. */
  std::unique_ptr<zmq::socket_t> socket_;
  /** The ZMQ socket routing ID. */
  std::string routing_id_;
};

/**
 * Messenger handles all the network aspects of sending and receiving messages.
 * Logic based on the messages is is deferred to MessengerLogic.
 *
 * @see messenger.cpp for a crash course on ZeroMQ, the current backing implementation.
 */
class Messenger : public common::DedicatedThreadTask {
 public:
  /**
   * Create a new Messenger that uses the given logic layer.
   * @param messenger_logic The logic layer of the messenger.
   */
  explicit Messenger(common::ManagedPointer<MessengerLogic> messenger_logic);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~Messenger();

  /** Run the main server loop, which dispatches messages received to the MessengerLogic layer. */
  void RunTask() override;

  /** Terminate the Messenger. */
  void Terminate() override;

  /**
   * Listen for new connections on the specified target destination.
   *
   * @warning           TODO(WAN): figure out what bad things happen if you give it a ConnectionDestination that is
   *                     already in use. I don't think this is a problem that is likely to occur because all our
   *                     destinations are known at compile time and we don't have too many right now, but I should
   *                     fix this at some point. I am reluctant to add a set of destinations just for this though..
   *
   * @param target      The destination to listen on for new connections.
   */
  void ListenForConnection(const ConnectionDestination &target);

  /**
   * Connect to the specified target destination, optionally providing an identity that we want to be known by.
   *
   * @param target      The destination to be connected to.
   * @param identity    An optional string to identify the connection by. See warning!
   * @return            A new ConnectionId. See warning!
   *
   * @warning           Identities must be unique to the Messenger instance that you are connecting to!
   * @warning           DO NOT USE THIS ConnectionId FROM A DIFFERENT THREAD THAN THE CALLER OF THIS FUNCTION!
   *                    Make a new connection instead, connections are cheap.
   */
  ConnectionId MakeConnection(const ConnectionDestination &target, std::optional<std::string> identity);

  /**
   * Send a message through the specified connection id.
   *
   * @warning   Remember that ConnectionId can only be used from the same thread that created it!
   *
   * @param connection_id   The connection to send the message over.
   * @param message         The message to be sent.
   */
  void SendMessage(common::ManagedPointer<ConnectionId> connection_id, std::string message);

 private:
  static constexpr int MESSENGER_PORT = 9022;
  static constexpr const char *MESSENGER_DEFAULT_TCP = "tcp://*:9022";
  static constexpr const char *MESSENGER_DEFAULT_IPC = "ipc:///tmp/noisepage-ipc0";
  static constexpr const char *MESSENGER_DEFAULT_INPROC = "inproc://noisepage-inproc";

  /** The main server loop. */
  void ServerLoop();

  common::ManagedPointer<MessengerLogic> messenger_logic_;
  std::unique_ptr<zmq::context_t> zmq_ctx_;
  std::unique_ptr<zmq::socket_t> zmq_default_socket_;
  bool messenger_running_ = false;
  uint32_t connection_id_count_ = 0;
};

/**
 * MessengerOwner is the entry point to the Messenger system.
 * MessengerOwner is responsible for instantiating the Messenger and "owning" it.
 * TODO(WAN): owning is a bad term, the Messenger is actually owned by the DedicatedThreadRegistry.
 */
class MessengerOwner : public common::DedicatedThreadOwner {
 public:
  /**
   * Create and run a new Messenger (which is a DedicatedThreadTask) on the specified thread registry.
   * @param thread_registry The registry in which the Messenger will be registered.
   */
  explicit MessengerOwner(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry);

  /** @return The owned messenger. */
  common::ManagedPointer<Messenger> GetMessenger() const { return messenger_; }

 private:
  MessengerLogic logic_;
  common::ManagedPointer<Messenger> messenger_;
};

}  // namespace terrier::messenger
