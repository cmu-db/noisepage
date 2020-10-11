#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"

// All zmq objects are forward-declared and allocated on the heap.
// This is to avoid leaking zmq headers into the rest of the system.
// The prediction is that doing this won't hurt performance too much.
namespace zmq {
class context_t;
class socket_t;
}  // namespace zmq

namespace terrier::messenger {

class ConnectionDestination;
class Messenger;
class MessengerPolledSockets;

/** ConnectionId is an abstraction around establishing connections. */
class ConnectionId {
 public:
  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~ConnectionId();

 private:
  friend Messenger;
  /**
   * Create a new ConnectionId that is connected to the specified target.
   * @param messenger   The messenger that owns this connection ID.
   * @param target      The target to be connected to.
   * @param identity    The name that the connection should have.
   */
  explicit ConnectionId(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                        const std::string &identity);

  /** The ZMQ socket. */
  std::unique_ptr<zmq::socket_t> socket_;
  /** The ZMQ socket routing ID. */
  std::string routing_id_;
  /** The target that was connected to. Useful for debugging. */
  std::string target_;
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
   * All messages can take in a callback function to be invoked when a reply is received.
   * Arg 1  :   std::string_view, sender identity.
   * Arg 2  :   std::string_view, the message itself.
   */
  using CallbackFn = std::function<void(std::string_view, std::string_view)>;

  /** @return The default TCP endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointTCP(const uint16_t port);
  /** @return The default IPC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointIPC(const uint16_t port);
  /** @return The default INPROC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointINPROC(const uint16_t port);

  /**
   * Create a new Messenger, listening to the default endpoints on the given port.
   * @param port        The port that determines the default endpoints.
   * @param identity    The identity that this Messenger instance is known by. See warning!
   *
   * @warning           Identity must be unique across all instances of Messengers.
   */
  explicit Messenger(const uint16_t port, std::string identity);

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
   * Connect to the specified target destination.
   *
   * @param target      The destination to be connected to.
   * @return            A new ConnectionId. See warning!
   *
   * @warning           DO NOT USE THIS ConnectionId FROM A DIFFERENT THREAD THAN THE CALLER OF THIS FUNCTION!
   *                    Make a new connection instead, connections are cheap.
   */
  ConnectionId MakeConnection(const ConnectionDestination &target);

  /**
   * Send a message through the specified connection id.
   *
   * @warning   Remember that ConnectionId can only be used from the same thread that created it!
   *
   * @param connection_id   The connection to send the message over.
   * @param message         The message to be sent.
   * @param callback        The callback function to be invoked on the response.
   */
  void SendMessage(common::ManagedPointer<ConnectionId> connection_id, std::string message, CallbackFn callback);

  /**
   * Send a message through the specified connection id.
   *
   * @warning   Remember that ConnectionId can only be used from the same thread that created it!
   *
   * @param connection_id   The connection to send the message over.
   * @param message         The message to be sent.
   * @param callback        The callback function to be invoked on the response.
   * @param special_fn_id   A special identifier pre-registered on the server that the server should invoke on receipt.
   */
  void SendMessage(common::ManagedPointer<ConnectionId> connection_id, std::string message, CallbackFn callback,
                   uint64_t special_fn_id);

 private:
  friend ConnectionId;
  static constexpr const char *MESSENGER_DEFAULT_TCP = "*";
  static constexpr const char *MESSENGER_DEFAULT_IPC = "/tmp/noisepage-ipc0-{}";
  static constexpr const char *MESSENGER_DEFAULT_INPROC = "noisepage-inproc-{}";

  /** The main server loop. */
  void ServerLoop();

  /** The port that is used for all default endpoints. */
  const uint16_t port_;
  /** The identity that this instance of the Messenger is known by. */
  const std::string identity_;

  std::unique_ptr<zmq::context_t> zmq_ctx_;
  std::unique_ptr<zmq::socket_t> zmq_default_socket_;
  std::unique_ptr<MessengerPolledSockets> polled_sockets_;
  std::unordered_map<uint64_t, CallbackFn> callbacks_;
  bool messenger_running_ = false;
  uint32_t connection_id_count_ = 0;
  /** The message ID that gets automatically prefixed to messages. */
  uint64_t message_id_ = 1;
};

/**
 * MessengerManager is the entry point to the Messenger system.
 * MessengerManager is responsible for instantiating the Messenger and then registering the Messenger with the
 * DedicatedThreadRegistry.
 */
class MessengerManager : public common::DedicatedThreadOwner {
 public:
  /**
   * Create and run a new Messenger (which is a DedicatedThreadTask) on the specified thread registry.
   * @param thread_registry The registry in which the Messenger will be registered.
   * @param port            The port on which the Messenger will listen by default.
   * @param identity        The name that this Messenger will be known by. Must be unique across all instances!
   */
  explicit MessengerManager(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                            const uint16_t port, const std::string &identity);

  /** @return The Messenger being managed. */
  common::ManagedPointer<Messenger> GetMessenger() const { return messenger_; }

 private:
  common::ManagedPointer<Messenger> messenger_;
};

}  // namespace terrier::messenger
