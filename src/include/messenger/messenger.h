#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <vector>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger_defs.h"

// All zmq objects are forward-declared and allocated on the heap.
// This is to avoid leaking zmq headers into the rest of the system.
// The prediction is that doing this won't hurt performance too much.
namespace zmq {
class context_t;
class socket_t;
}  // namespace zmq

namespace noisepage::messenger {

class ConnectionDestination;
class Messenger;
class MessengerPolledSockets;
class ZmqMessage;

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
  /** The target that was connected to. */
  std::string target_name_;
};

/** ConnectionRouter represents a new endpoint opened up on the Messenger to listen for connections. */
class ConnectionRouter {
 public:
  /**
   * Create a new ConnectionRouter that listens for incoming connnections.
   * @param messenger   The messenger that owns this connection router.
   * @param target      The target to listen on.
   * @param identity    The name that the router should have.
   * @param callback    The server loop for this connection router.
   */
  explicit ConnectionRouter(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                            const std::string &identity, CallbackFn callback);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~ConnectionRouter();

  /** @return The identity of this connection router. */
  const std::string &GetIdentity() const { return identity_; }

 private:
  friend Messenger;

  /** The ZMQ socket. */
  std::unique_ptr<zmq::socket_t> socket_;
  /** The callback to be invoked on all received messages. */
  CallbackFn callback_;
  /** The identity of this router. */
  std::string identity_;
};

/**
 * Messenger handles all the network aspects of sending and receiving messages.
 * Logic based on the messages is is deferred to MessengerLogic.
 *
 * @see messenger.cpp for a crash course on ZeroMQ, the current backing implementation.
 */
class Messenger : public common::DedicatedThreadTask {
 private:
  /**
   * A ConnectionRouter that should be added. Unfortunately, ZeroMQ is not thread safe and this must be done from
   * the main serverloop thread for poll to function correctly.
   */
  struct RouterToBeAdded {
    ConnectionDestination target_;
    std::string identity_;
    CallbackFn callback_;
  };

 public:
  /** Builtin callbacks useful for testing. */
  enum class BuiltinCallback : uint8_t { NOOP = 0, ECHO, NUM_BUILTIN_CALLBACKS };

  /** @return The default TCP endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointTCP(std::string target_name, uint16_t port);
  /** @return The default IPC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointIPC(std::string target_name, uint16_t port);
  /** @return The default INPROC endpoint for a Messenger on the given port. */
  static ConnectionDestination GetEndpointINPROC(std::string target_name, uint16_t port);

  /**
   * Create a new Messenger, listening to the default endpoints on the given port.
   * @param port        The port that determines the default endpoints.
   * @param identity    The identity that this Messenger instance is known by. See warning!
   *
   * @warning           Identity must be unique across all instances of Messengers.
   */
  explicit Messenger(uint16_t port, std::string identity);

  /** An explicit destructor is necessary because of the unique_ptr around a forward-declared type. */
  ~Messenger() override;

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
   * @param identity    The identity to listen as.
   * @param callback    The server loop for all messages received.
   */
  void ListenForConnection(const ConnectionDestination &target, const std::string &identity, CallbackFn callback);

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
   * @param recv_cb_id      The receiver's callback ID, e.g., sent in response or to invoke preregistered functions.
   *                        To invoke preregistered functions, use static_cast<uint8_t>(Messenger::BuiltinCallback).
   */
  void SendMessage(common::ManagedPointer<ConnectionId> connection_id, const std::string &message, CallbackFn callback,
                   uint64_t recv_cb_id);

  /**
   * Send a message through the specified connection router.
   *
   * @warning   A ConnectionRouter only knows about receivers that are directly connected to it.
   *            After obtaining a ConnectionRouter from ListenForConnection,
   *
   * @param router_id       The connection router to send the message over.
   * @param recv_id         The ID of the receiver.
   * @param message         The message to be sent.
   * @param callback        The callback function to be invoked on the response.
   * @param recv_cb_id      The receiver's callback ID, e.g., sent in response or to invoke preregistered functions.
   *                        To invoke preregistered functions, use static_cast<uint8_t>(Messenger::BuiltinCallback).
   */
  void SendMessage(common::ManagedPointer<ConnectionRouter> router_id, const std::string &recv_id,
                   const std::string &message, CallbackFn callback, uint64_t recv_cb_id);

  /** @return The ConnectionRouter with the specified router_id. Created by ListenForConnection. */
  common::ManagedPointer<ConnectionRouter> GetConnectionRouter(const std::string &router_id);

 private:
  friend ConnectionId;
  friend ConnectionRouter;

  static constexpr const char *MESSENGER_DEFAULT_TCP = "*";
  static constexpr const char *MESSENGER_DEFAULT_IPC = "/tmp/noisepage-ipc-{}";
  static constexpr const char *MESSENGER_DEFAULT_INPROC = "noisepage-inproc-{}";
  static constexpr const std::chrono::milliseconds MESSENGER_POLL_TIMER = std::chrono::seconds(2);

  /** @return The next ID to be used when sending messages. */
  uint64_t GetNextSendMessageId();

  /** The main server loop. */
  void ServerLoop();

  /** Processes messages. Responsible for special callback functions specified by message ID. */
  void ProcessMessage(const ZmqMessage &msg);

  /** The port that is used for all default endpoints. */
  const uint16_t port_;
  /** The identity that this instance of the Messenger is known by. */
  const std::string identity_;

  std::unique_ptr<zmq::context_t> zmq_ctx_;
  std::unique_ptr<zmq::socket_t> zmq_default_socket_;
  std::unique_ptr<MessengerPolledSockets> polled_sockets_;
  std::unordered_map<uint64_t, CallbackFn> callbacks_;

  std::vector<RouterToBeAdded> routers_to_be_added_;
  std::mutex routers_add_mutex_;
  std::unordered_map<std::string, std::unique_ptr<ConnectionRouter>> routers_;

  std::mutex callbacks_mutex_;
  bool is_messenger_running_ = false;
  /** The message ID that gets automatically prefixed to messages. */
  std::atomic<uint64_t> message_id_{static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1};
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
  explicit MessengerManager(common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry, uint16_t port,
                            const std::string &identity);

  /** @return The Messenger being managed. */
  common::ManagedPointer<Messenger> GetMessenger() const { return messenger_; }

 private:
  common::ManagedPointer<Messenger> messenger_;
};

}  // namespace noisepage::messenger
